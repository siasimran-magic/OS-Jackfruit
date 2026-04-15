/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 * Tasks 1-4 Implementation (With Memory Monitoring)
 */

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 4096
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    int stop_requested;
    int soft_limit_exceeded;
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static supervisor_ctx_t *g_ctx = NULL;

/* ==================== TASK 3: BOUNDED BUFFER IMPLEMENTATION ==================== */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;
    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    fprintf(stderr, "[bounded_buffer] Initialized (capacity=%d)\n", LOG_BUFFER_CAPACITY);
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    fprintf(stderr, "[bounded_buffer] Shutdown signal sent\n");
}

/* TASK 3: Producer - push log entry into bounded buffer */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait if buffer is full and not shutting down */
    while (buffer->count >= LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        fprintf(stderr, "[producer] Buffer full, waiting...\n");
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    memcpy(&buffer->items[buffer->tail], item, sizeof(log_item_t));
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    fprintf(stderr, "[producer] Pushed log item for %s (count=%zu)\n", 
            item->container_id, buffer->count);

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* TASK 3: Consumer - pop log entry from bounded buffer */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    if (buffer->count == 0) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    memcpy(item, &buffer->items[buffer->head], sizeof(log_item_t));
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    fprintf(stderr, "[consumer] Popped log item for %s (count=%zu)\n", 
            item->container_id, buffer->count);

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* TASK 3: Logger consumer thread - writes log items to per-container files */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    int rc;
    FILE *logfile;
    char logpath[PATH_MAX];

    fprintf(stderr, "[logger] Consumer thread started\n");
    fflush(stderr);

    while (1) {
        rc = bounded_buffer_pop(&ctx->log_buffer, &item);
        if (rc < 0) {
            fprintf(stderr, "[logger] Shutdown signal received\n");
            fflush(stderr);
            break;
        }

        snprintf(logpath, sizeof(logpath), "%s/%s.log", LOG_DIR, item.container_id);

        logfile = fopen(logpath, "a");
        if (!logfile) {
            fprintf(stderr, "[logger] Failed to open %s: %s\n", logpath, strerror(errno));
            fflush(stderr);
            continue;
        }

        if (fwrite(item.data, 1, item.length, logfile) != item.length) {
            fprintf(stderr, "[logger] Failed to write to %s\n", logpath);
            fflush(stderr);
        }
        fflush(logfile);
        fclose(logfile);
    }

    fprintf(stderr, "[logger] Consumer thread exiting\n");
    fflush(stderr);
    return NULL;
}

/* ==================== TASK 1 & 2: CONTAINER MANAGEMENT ==================== */

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static container_record_t *find_container(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *curr = ctx->containers;
    while (curr) {
        if (strcmp(curr->id, id) == 0)
            return curr;
        curr = curr->next;
    }
    return NULL;
}

static container_record_t *add_container(supervisor_ctx_t *ctx, const char *id, pid_t pid)
{
    container_record_t *rec = malloc(sizeof(*rec));
    if (!rec)
        return NULL;

    memset(rec, 0, sizeof(*rec));
    
    memset(rec->id, 0, sizeof(rec->id));
    strncpy(rec->id, id, CONTAINER_ID_LEN - 1);
    
    rec->host_pid = pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_STARTING;
    rec->exit_code = -1;
    rec->exit_signal = -1;
    rec->soft_limit_exceeded = 0;
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, id);

    rec->next = ctx->containers;
    ctx->containers = rec;

    fprintf(stderr, "[supervisor] Added container record: %s (pid=%d)\n", id, pid);
    fflush(stderr);
    return rec;
}

/* ==================== TASK 1: CONTAINER ISOLATION ==================== */

/* TASK 1: Clone child entrypoint - sets up isolated container */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;
    char hostname[256];

    fprintf(stderr, "[child] PID %d starting container %s\n", getpid(), cfg->id);
    fflush(stderr);

    /* Set container hostname */
    snprintf(hostname, sizeof(hostname), "container-%s", cfg->id);
    if (sethostname(hostname, strlen(hostname)) < 0) {
        perror("sethostname");
        return 1;
    }

    /* Change root to container rootfs */
    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") < 0) {
        perror("chdir");
        return 1;
    }

    /* Mount /proc inside container */
    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        perror("mount /proc");
        return 1;
    }

    /* Set nice value if requested */
    if (cfg->nice_value != 0) {
        if (nice(cfg->nice_value) < 0) {
            perror("nice");
            return 1;
        }
    }

    /* Redirect stdout/stderr to log pipe */
    if (cfg->log_write_fd >= 0) {
        if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0) {
            perror("dup2 stdout");
            return 1;
        }
        if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
            perror("dup2 stderr");
            return 1;
        }
        close(cfg->log_write_fd);
    }

    /* Execute command via shell */
    char *args[] = {"/bin/sh", "-c", cfg->command, NULL};
    execv("/bin/sh", args);

    perror("execv");
    return 1;
}

/* ==================== TASK 2: SIGNAL HANDLING ==================== */

/* TASK 2: SIGCHLD handler - reap exited children */
static void sigchld_handler(int sig)
{
    (void)sig;
    if (g_ctx) {
        pid_t pid;
        int status;

        while ((pid = waitpid(-1, &status, WNOHANG | WUNTRACED)) > 0) {
            pthread_mutex_lock(&g_ctx->metadata_lock);

            container_record_t *curr = g_ctx->containers;
            while (curr) {
                if (curr->host_pid == pid) {
                    if (WIFEXITED(status)) {
                        curr->exit_code = WEXITSTATUS(status);
                        if (curr->stop_requested) {
                            curr->state = CONTAINER_STOPPED;
                        } else {
                            curr->state = CONTAINER_EXITED;
                        }
                    } else if (WIFSIGNALED(status)) {
                        curr->exit_signal = WTERMSIG(status);
                        if (curr->exit_signal == SIGKILL) {
                            curr->state = CONTAINER_KILLED;
                        } else {
                            curr->state = CONTAINER_STOPPED;
                        }
                    }
                    fprintf(stderr, "[supervisor] Container %s reaped (pid=%d, state=%s, exit_code=%d, signal=%d)\n",
                            curr->id, pid, state_to_string(curr->state), curr->exit_code, curr->exit_signal);
                    fflush(stderr);
                    break;
                }
                curr = curr->next;
            }

            pthread_mutex_unlock(&g_ctx->metadata_lock);
        }
    }
}

/* TASK 2: SIGINT/SIGTERM handler - graceful shutdown */
static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx) {
        g_ctx->should_stop = 1;
        fprintf(stderr, "\n[supervisor] Shutdown signal (SIGINT/SIGTERM) received, beginning graceful shutdown...\n");
        fflush(stderr);
    }
}

/* ==================== TASK 2 & 4: HELPER FUNCTIONS ==================== */

int register_with_monitor(int monitor_fd, const char *container_id, pid_t host_pid,
                          unsigned long soft_limit_bytes, unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    fprintf(stderr, "[supervisor] Registered container %s with monitor (pid=%d, soft=%lu MB, hard=%lu MB)\n", 
            container_id, host_pid, soft_limit_bytes >> 20, hard_limit_bytes >> 20);
    fflush(stderr);
    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    fprintf(stderr, "[supervisor] Unregistered container %s from monitor\n", container_id);
    fflush(stderr);
    return 0;
}

/* TASK 4: Query container memory status from monitor */
int query_monitor(int monitor_fd, int pid, struct monitor_query *query)
{
    memset(query, 0, sizeof(*query));
    query->pid = pid;

    if (ioctl(monitor_fd, MONITOR_QUERY, query) < 0)
        return -1;

    return 0;
}

/* ==================== TASK 2: COMMAND HANDLERS ==================== */

/* TASK 4: Updated PS command with memory monitoring */
static int handle_ps_command(supervisor_ctx_t *ctx, char *output_buffer, size_t output_size)
{
    container_record_t *curr;
    time_t now = time(NULL);
    int len = 0;

    pthread_mutex_lock(&ctx->metadata_lock);

    len += snprintf(output_buffer + len, output_size - len, 
           "\n%-15s %-8s %-12s %-10s %-15s %-12s %-10s\n",
           "CONTAINER", "PID", "STATE", "UPTIME(s)", "SOFT/HARD MB", "RSS MB", "EXIT");
    len += snprintf(output_buffer + len, output_size - len, 
           "%-15s %-8s %-12s %-10s %-15s %-12s %-10s\n",
           "=========", "===", "=====", "=========", "=============", "=======", "====");

    curr = ctx->containers;
    while (curr) {
        int uptime = (int)(now - curr->started_at);
        char exit_info[32] = "-";
        unsigned long rss_mb = 0;

        if (curr->exit_code >= 0) {
            snprintf(exit_info, sizeof(exit_info), "code:%d", curr->exit_code);
        } else if (curr->exit_signal >= 0) {
            snprintf(exit_info, sizeof(exit_info), "sig:%d", curr->exit_signal);
        }

        /* Query memory from monitor if available and container is running */
        if (ctx->monitor_fd >= 0 && curr->state == CONTAINER_RUNNING) {
            struct monitor_query mq;
            if (query_monitor(ctx->monitor_fd, curr->host_pid, &mq) == 0) {
                rss_mb = mq.rss_bytes >> 20;
                /* Update soft limit exceeded flag */
                if (mq.soft_limit_exceeded) {
                    curr->soft_limit_exceeded = 1;
                }
            }
        }

        len += snprintf(output_buffer + len, output_size - len,
               "%-15s %-8d %-12s %-10d %lu/%-8lu %-12lu %s\n",
               curr->id, curr->host_pid, state_to_string(curr->state),
               uptime, curr->soft_limit_bytes >> 20, curr->hard_limit_bytes >> 20,
               rss_mb, exit_info);

        curr = curr->next;
    }
    len += snprintf(output_buffer + len, output_size - len, "\n");

    pthread_mutex_unlock(&ctx->metadata_lock);
    return len;
}

static void handle_logs_command(supervisor_ctx_t *ctx, const char *container_id)
{
    FILE *logfile;
    char logpath[PATH_MAX];
    char buffer[4096];
    size_t n;

    (void)ctx;

    snprintf(logpath, sizeof(logpath), "%s/%s.log", LOG_DIR, container_id);

    logfile = fopen(logpath, "r");
    if (!logfile) {
        fprintf(stderr, "Log file not found: %s\n", logpath);
        fflush(stderr);
        return;
    }

    fprintf(stderr, "[logs] Reading from %s:\n", logpath);
    fflush(stderr);
    fprintf(stderr, "---\n");
    fflush(stderr);

    while ((n = fread(buffer, 1, sizeof(buffer), logfile)) > 0) {
        fwrite(buffer, 1, n, stdout);
        fflush(stdout);
    }

    fprintf(stderr, "---\n");
    fflush(stderr);
    fclose(logfile);
}

static void handle_stop_command(supervisor_ctx_t *ctx, const char *container_id)
{
    container_record_t *rec;

    pthread_mutex_lock(&ctx->metadata_lock);

    rec = find_container(ctx, container_id);
    if (!rec) {
        fprintf(stderr, "Container not found: %s\n", container_id);
        fflush(stderr);
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    if (rec->state != CONTAINER_RUNNING) {
        fprintf(stderr, "Container %s is not running (state: %s)\n",
                container_id, state_to_string(rec->state));
        fflush(stderr);
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    rec->stop_requested = 1;
    fprintf(stderr, "[supervisor] Sending SIGTERM to container %s (pid=%d)\n",
            container_id, rec->host_pid);
    fflush(stderr);

    if (kill(rec->host_pid, SIGTERM) < 0) {
        perror("kill");
    }

    pthread_mutex_unlock(&ctx->metadata_lock);
}

/* Pipe reader thread function */
typedef struct {
    int fd;
    supervisor_ctx_t *ctx;
    char container_id[CONTAINER_ID_LEN];
} pipe_reader_arg_t;

void *pipe_reader_thread(void *arg)
{
    pipe_reader_arg_t *parg = (pipe_reader_arg_t *)arg;
    char buffer[LOG_CHUNK_SIZE];
    ssize_t n;
    log_item_t item;

    fprintf(stderr, "[pipe_reader] Thread started for %s\n", parg->container_id);
    fflush(stderr);

    while ((n = read(parg->fd, buffer, sizeof(buffer) - 1)) > 0) {
        memset(&item, 0, sizeof(item));
        memset(item.container_id, 0, sizeof(item.container_id));
        strncpy(item.container_id, parg->container_id, CONTAINER_ID_LEN - 1);
        item.length = n;
        memcpy(item.data, buffer, n);
        bounded_buffer_push(&parg->ctx->log_buffer, &item);
    }

    fprintf(stderr, "[pipe_reader] Thread exiting for %s\n", parg->container_id);
    fflush(stderr);
    close(parg->fd);
    free(parg);
    return NULL;
}

/* TASK 1 & 2: Handle START command - launch new container */
static void handle_start_command(supervisor_ctx_t *ctx, const control_request_t *req)
{
    pid_t child_pid;
    container_record_t *rec;
    child_config_t cfg;
    char stack[STACK_SIZE];
    int rc;
    int pipefd[2];
    pthread_t pipe_reader;
    pipe_reader_arg_t *parg;

    /* Check if container already exists */
    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container(ctx, req->container_id)) {
        fprintf(stderr, "Container already exists: %s\n", req->container_id);
        fflush(stderr);
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Create log pipe for capturing container output */
    if (pipe(pipefd) < 0) {
        perror("pipe");
        return;
    }

    memset(&cfg, 0, sizeof(cfg));
    memset(cfg.id, 0, sizeof(cfg.id));
    strncpy(cfg.id, req->container_id, CONTAINER_ID_LEN - 1);
    
    memset(cfg.rootfs, 0, sizeof(cfg.rootfs));
    strncpy(cfg.rootfs, req->rootfs, PATH_MAX - 1);
    
    memset(cfg.command, 0, sizeof(cfg.command));
    strncpy(cfg.command, req->command, CHILD_COMMAND_LEN - 1);
    
    cfg.nice_value = req->nice_value;
    cfg.log_write_fd = pipefd[1];

    fprintf(stderr, "[supervisor] Cloning container %s with command: %s\n",
            req->container_id, req->command);
    fflush(stderr);

    /* Clone new container process with isolated namespaces */
    child_pid = clone(child_fn, stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      &cfg);

    if (child_pid < 0) {
        perror("clone");
        close(pipefd[0]);
        close(pipefd[1]);
        return;
    }

    close(pipefd[1]);

    /* Add to container list */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec = add_container(ctx, req->container_id, child_pid);
    if (rec) {
        rec->soft_limit_bytes = req->soft_limit_bytes;
        rec->hard_limit_bytes = req->hard_limit_bytes;
        rec->state = CONTAINER_RUNNING;
    }
    fprintf(stderr, "[supervisor] Started container %s (pid=%d, soft=%lu MB, hard=%lu MB)\n",
            req->container_id, child_pid,
            req->soft_limit_bytes >> 20, req->hard_limit_bytes >> 20);
    fflush(stderr);
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Register with kernel monitor */
    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd, req->container_id, child_pid,
                                  req->soft_limit_bytes, req->hard_limit_bytes) < 0) {
            fprintf(stderr, "[supervisor] Failed to register with monitor (device not available)\n");
            fflush(stderr);
        }
    }

    /* Create pipe reader thread */
    parg = malloc(sizeof(*parg));
    if (parg) {
        parg->fd = pipefd[0];
        parg->ctx = ctx;
        memset(parg->container_id, 0, sizeof(parg->container_id));
        strncpy(parg->container_id, req->container_id, CONTAINER_ID_LEN - 1);

        rc = pthread_create(&pipe_reader, NULL, pipe_reader_thread, parg);
        if (rc != 0) {
            fprintf(stderr, "[supervisor] Failed to create pipe reader thread\n");
            fflush(stderr);
            free(parg);
        } else {
            pthread_detach(pipe_reader);
        }
    }
}

/* Parse command-line flags */
static int parse_mib_flag(const char *flag, const char *value, unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req, int argc, char *argv[], int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr, "Invalid value for --nice (expected -20..19): %s\n", argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

/* ==================== TASK 2: SUPERVISOR EVENT LOOP ==================== */

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;
    struct sigaction sa;
    struct sockaddr_un addr;
    int client_fd;
    socklen_t addrlen;
    control_request_t req;
    control_response_t resp;

    (void)rootfs;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    g_ctx = &ctx;

    /* Create log directory */
    mkdir(LOG_DIR, 0755);

    /* Initialize metadata lock */
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    /* Initialize bounded buffer */
    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* Open kernel monitor device */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr, "[supervisor] Warning: /dev/container_monitor not available (Task 4 disabled)\n");
        fflush(stderr);
        ctx.monitor_fd = -1;
    } else {
        fprintf(stderr, "[supervisor] Opened /dev/container_monitor for memory monitoring\n");
        fflush(stderr);
    }

    /* Create UNIX domain socket for IPC */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        goto cleanup;
    }

    if (listen(ctx.server_fd, 5) < 0) {
        perror("listen");
        goto cleanup;
    }

    fprintf(stderr, "[supervisor] Listening on %s\n", CONTROL_PATH);
    fflush(stderr);

    /* Install signal handlers */
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGCHLD, &sa, NULL);

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigterm_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* Start logging consumer thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        goto cleanup;
    }

    fprintf(stderr, "[supervisor] Supervisor started (PID=%d)\n", getpid());
    fflush(stderr);

    /* Main event loop */
    while (!ctx.should_stop) {
        fd_set readfds;
        struct timeval tv;

        FD_ZERO(&readfds);
        FD_SET(ctx.server_fd, &readfds);

        tv.tv_sec = 1;
        tv.tv_usec = 0;

        rc = select(ctx.server_fd + 1, &readfds, NULL, NULL, &tv);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }

        if (rc == 0)
            continue;

        if (!FD_ISSET(ctx.server_fd, &readfds))
            continue;

        addrlen = sizeof(addr);
        client_fd = accept(ctx.server_fd, (struct sockaddr *)&addr, &addrlen);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        /* Receive control request */
        if (recv(client_fd, &req, sizeof(req), 0) != sizeof(req)) {
            fprintf(stderr, "Failed to receive control request\n");
            fflush(stderr);
            close(client_fd);
            continue;
        }

        memset(&resp, 0, sizeof(resp));
        resp.status = 0;

        /* Dispatch command */
        switch (req.kind) {
        case CMD_START:
            handle_start_command(&ctx, &req);
            snprintf(resp.message, sizeof(resp.message), "Container %s started", 
                     req.container_id);
            break;

        case CMD_PS:
            {
                int ps_len = handle_ps_command(&ctx, resp.message, sizeof(resp.message));
                if (ps_len <= 0) {
                    snprintf(resp.message, sizeof(resp.message), "No containers running\n");
                }
            }
            break;

        case CMD_LOGS:
            handle_logs_command(&ctx, req.container_id);
            snprintf(resp.message, sizeof(resp.message), "OK");
            break;

        case CMD_STOP:
            handle_stop_command(&ctx, req.container_id);
            snprintf(resp.message, sizeof(resp.message), 
                     "Container %s stop requested", req.container_id);
            break;

        default:
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "Unknown command");
            break;
        }

        /* Send response */
        send(client_fd, &resp, sizeof(resp), 0);
        close(client_fd);
    }

    fprintf(stderr, "[supervisor] Shutting down...\n");
    fflush(stderr);

cleanup:
    /* Unregister all containers from monitor before terminating */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *curr = ctx.containers;
    while (curr) {
        if (ctx.monitor_fd >= 0) {
            unregister_from_monitor(ctx.monitor_fd, curr->id, curr->host_pid);
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Terminate all running containers */
    fprintf(stderr, "[supervisor] Terminating all running containers...\n");
    fflush(stderr);
    
    pthread_mutex_lock(&ctx.metadata_lock);
    curr = ctx.containers;
    while (curr) {
        if (curr->state == CONTAINER_RUNNING) {
            fprintf(stderr, "[supervisor] Sending SIGTERM to container %s (pid=%d)\n", curr->id, curr->host_pid);
            fflush(stderr);
            kill(curr->host_pid, SIGTERM);
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Give containers time to exit gracefully */
    sleep(2);

    /* Kill any remaining containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    curr = ctx.containers;
    while (curr) {
        if (curr->state == CONTAINER_RUNNING) {
            fprintf(stderr, "[supervisor] Sending SIGKILL to container %s (pid=%d)\n", curr->id, curr->host_pid);
            fflush(stderr);
            kill(curr->host_pid, SIGKILL);
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Signal logger to shutdown */
    fprintf(stderr, "[supervisor] Signaling logger to shutdown...\n");
    fflush(stderr);
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    /* Cleanup */
    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    unlink(CONTROL_PATH);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);

    fprintf(stderr, "[supervisor] Supervisor exited cleanly\n");
    fflush(stderr);
    return 0;
}

/* ==================== TASK 2: CLIENT IPC ==================== */

static int send_control_request(const control_request_t *req)
{
    int sock;
    struct sockaddr_un addr;
    control_response_t resp;

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Failed to connect to supervisor: %s\n", strerror(errno));
        close(sock);
        return 1;
    }

    if (send(sock, req, sizeof(*req), 0) != sizeof(*req)) {
        fprintf(stderr, "Failed to send request\n");
        close(sock);
        return 1;
    }

    if (recv(sock, &resp, sizeof(resp), 0) != sizeof(resp)) {
        fprintf(stderr, "Failed to receive response\n");
        close(sock);
        return 1;
    }

    if (resp.status != 0) {
        fprintf(stderr, "Error: %s\n", resp.message);
        close(sock);
        return 1;
    }

    printf("%s\n", resp.message);
    close(sock);
    return 0;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <command> [args...]\n", argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);
    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();
    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    fprintf(stderr, "Unknown command: %s\n", argv[1]);
    return 1;
}
