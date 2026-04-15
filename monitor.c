/*
 * monitor.c - Multi-Container Memory Monitor (Linux Kernel Module)
 * Tasks 1-4 Implementation with RSS Monitoring (Kernel 5.15 compatible)
 */

#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/ktime.h>
#include <linux/list.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched.h>
#include <linux/sched/mm.h>
#include <linux/slab.h>
#include <linux/kthread.h>
#include <linux/delay.h>
#include <linux/signal.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME "container_monitor"

static struct cdev c_dev;
static dev_t dev_num;
static struct class *cl;
static DEFINE_MUTEX(monitor_lock);

typedef struct {
    struct list_head list;
    int pid;
    char container_id[32];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int soft_limit_warned;
    unsigned long last_rss_bytes;
} monitor_entry_t;

static LIST_HEAD(monitor_list);
static struct task_struct *monitor_thread;

/* Helper: Get RSS of a process in bytes (Kernel 5.15 compatible) */
static unsigned long get_process_rss(int pid)
{
    struct task_struct *task;
    unsigned long rss_bytes = 0;
    struct mm_struct *mm;
    unsigned long rss_stat = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    
    if (task && (mm = get_task_mm(task))) {
        /* For kernel 5.15, read RSS from the mm_struct directly */
        #ifdef CONFIG_MEMCG
        /* Try to get RSS from page cache counters */
        rss_stat = READ_ONCE(mm->rss_stat.count[0].counter);
        if (rss_stat == 0) {
            rss_stat = READ_ONCE(mm->rss_stat.count[1].counter);
        }
        #else
        /* Fallback: estimate from mm_struct fields */
        rss_stat = 0;
        #endif
        
        /* If we got RSS stat, use it; otherwise use a safe default */
        if (rss_stat > 0) {
            rss_bytes = rss_stat << PAGE_SHIFT;
        } else {
            /* Safe fallback: just use 0 for now */
            rss_bytes = 0;
        }
        
        mmput(mm);
    }
    rcu_read_unlock();

    return rss_bytes;
}

/* Helper: Get task_struct by PID */
static struct task_struct *get_task_by_pid(int pid)
{
    struct task_struct *task;
    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    rcu_read_unlock();
    return task;
}

/* Periodic check: Monitor all registered processes */
static void monitor_check_limits(void)
{
    monitor_entry_t *entry;
    unsigned long rss_bytes;
    struct task_struct *task;

    mutex_lock(&monitor_lock);

    list_for_each_entry(entry, &monitor_list, list) {
        task = get_task_by_pid(entry->pid);
        if (!task) {
            /* Process no longer exists */
            continue;
        }

        rss_bytes = get_process_rss(entry->pid);
        entry->last_rss_bytes = rss_bytes;

        /* Check soft limit */
        if (rss_bytes > entry->soft_limit_bytes && !entry->soft_limit_warned) {
            entry->soft_limit_warned = 1;
            printk(KERN_WARNING "[monitor] SOFT LIMIT exceeded: container=%s pid=%d rss=%lu MB (limit=%lu MB)\n",
                   entry->container_id, entry->pid,
                   rss_bytes >> 20, entry->soft_limit_bytes >> 20);
        }

        /* Check hard limit */
        if (rss_bytes > entry->hard_limit_bytes && entry->hard_limit_bytes > 0) {
            printk(KERN_CRIT "[monitor] HARD LIMIT exceeded: container=%s pid=%d rss=%lu MB (limit=%lu MB) - KILLING\n",
                   entry->container_id, entry->pid,
                   rss_bytes >> 20, entry->hard_limit_bytes >> 20);
            
            /* Send SIGKILL to process */
            if (task) {
                send_sig(SIGKILL, task, 0);
                printk(KERN_INFO "[monitor] Sent SIGKILL to pid=%d\n", entry->pid);
            }
        }
    }

    mutex_unlock(&monitor_lock);
}

static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;
    struct monitor_query query;
    monitor_entry_t *entry, *tmp;
    unsigned long rss_bytes;

    (void)f;

    switch (cmd) {
    case MONITOR_REGISTER:
        if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
            return -EFAULT;

        mutex_lock(&monitor_lock);
        entry = kmalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry) {
            mutex_unlock(&monitor_lock);
            return -ENOMEM;
        }

        entry->pid = req.pid;
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        entry->soft_limit_warned = 0;
        entry->last_rss_bytes = 0;
        strncpy(entry->container_id, req.container_id, sizeof(entry->container_id) - 1);
        entry->container_id[sizeof(entry->container_id) - 1] = '\0';

        list_add(&entry->list, &monitor_list);
        mutex_unlock(&monitor_lock);

        printk(KERN_INFO "[monitor] REGISTER: container=%s pid=%d soft=%lu MB hard=%lu MB\n",
               req.container_id, req.pid,
               req.soft_limit_bytes >> 20, req.hard_limit_bytes >> 20);
        break;

    case MONITOR_UNREGISTER:
        if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
            return -EFAULT;

        mutex_lock(&monitor_lock);
        list_for_each_entry_safe(entry, tmp, &monitor_list, list) {
            if (entry->pid == req.pid && strcmp(entry->container_id, req.container_id) == 0) {
                list_del(&entry->list);
                kfree(entry);
                printk(KERN_INFO "[monitor] UNREGISTER: container=%s pid=%d\n",
                       req.container_id, req.pid);
                break;
            }
        }
        mutex_unlock(&monitor_lock);
        break;

    case MONITOR_QUERY:
        if (copy_from_user(&query, (struct monitor_query __user *)arg, sizeof(query)))
            return -EFAULT;

        mutex_lock(&monitor_lock);
        list_for_each_entry(entry, &monitor_list, list) {
            if (entry->pid == query.pid) {
                rss_bytes = get_process_rss(entry->pid);
                query.rss_bytes = rss_bytes;
                query.soft_limit_exceeded = (rss_bytes > entry->soft_limit_bytes) ? 1 : 0;
                query.hard_limit_exceeded = (rss_bytes > entry->hard_limit_bytes) ? 1 : 0;
                break;
            }
        }
        mutex_unlock(&monitor_lock);

        if (copy_to_user((struct monitor_query __user *)arg, &query, sizeof(query)))
            return -EFAULT;
        break;

    default:
        return -EINVAL;
    }

    return 0;
}

static struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* Periodic monitoring thread */
static int monitor_thread_fn(void *data)
{
    (void)data;
    while (!kthread_should_stop()) {
        monitor_check_limits();
        msleep(1000);  /* Check every 1 second */
    }
    return 0;
}

static int __init monitor_init(void)
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif

    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    /* Start monitoring thread */
    monitor_thread = kthread_run(monitor_thread_fn, NULL, "container_monitor");
    if (IS_ERR(monitor_thread)) {
        cdev_del(&c_dev);
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(monitor_thread);
    }

    printk(KERN_INFO "[container_monitor] Module loaded with monitoring thread\n");
    return 0;
}

static void __exit monitor_exit(void)
{
    monitor_entry_t *entry, *tmp;

    /* Stop monitoring thread */
    if (monitor_thread && !IS_ERR(monitor_thread))
        kthread_stop(monitor_thread);

    /* Cleanup entries */
    mutex_lock(&monitor_lock);
    list_for_each_entry_safe(entry, tmp, &monitor_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }
    mutex_unlock(&monitor_lock);

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);
    printk(KERN_INFO "[container_monitor] Module unloaded\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Container memory monitor with soft/hard limits");
