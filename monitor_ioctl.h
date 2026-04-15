#ifndef MONITOR_IOCTL_H
#define MONITOR_IOCTL_H

#include <linux/ioctl.h>

#define MONITOR_MAGIC 'M'
#define MONITOR_REGISTER _IOW(MONITOR_MAGIC, 1, struct monitor_request)
#define MONITOR_UNREGISTER _IOW(MONITOR_MAGIC, 2, struct monitor_request)
#define MONITOR_QUERY _IOWR(MONITOR_MAGIC, 3, struct monitor_query)

struct monitor_request {
    int pid;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    char container_id[32];
};

struct monitor_query {
    int pid;
    unsigned long rss_bytes;
    int soft_limit_exceeded;
    int hard_limit_exceeded;
};

#endif /* MONITOR_IOCTL_H */
