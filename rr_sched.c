/*
 * Project 1: Simple scheduling (Round-Robin) -- parent = OS scheduler,
 *            10 children = simulated user processes.
 *
 * Build : gcc -Wall -O2 -o rr_sched rr_sched.c
 * Run   : ./rr_sched [tick_usec=10000] [max_ticks=10000]
 *         (10 ms tick x 10,000 ticks = 100 s, so every process lives > 1 min)
 * Output: schedule_dump.txt
 *
 * Mechanisms
 *   - setitimer(ITIMER_REAL) + SIGALRM : periodic timer tick
 *   - System V message queues (msgget/msgsnd/msgrcv) : parent <-> child IPC
 *       q_cpu : parent -> child  "you got one tick of CPU"
 *       q_io  : child  -> parent "my CPU burst finished, I/O burst = X"
 *   - run-queue (ready) and wait-queue (I/O) kept by the parent
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/time.h>
#include <sys/wait.h>

#define NPROC        10
#define QUANTUM      5          /* time quantum, in ticks                */
#define CPU_MIN      3          /* random CPU burst range (ticks)        */
#define CPU_MAX      30
#define IO_MIN       5          /* random I/O burst range (ticks)        */
#define IO_MAX       40
#define DEF_TICK_US  10000      /* 10 ms timer tick                      */
#define DEF_MAXTICK  10000
#define DUMP_FILE    "schedule_dump.txt"

/* ---------- IPC message formats ---------- */
struct msg_cpu {                /* parent -> child */
    long mtype;                 /* = child index + 1 */
    int  tick;                  /* current time tick (informational) */
};
struct msg_io {                 /* child -> parent */
    long mtype;                 /* = child index + 1 */
    pid_t pid;
    int  io_burst;              /* length of the I/O burst that starts now */
    int  next_cpu_burst;        /* CPU burst that follows the I/O          */
};

/* ---------- parent-side PCB ---------- */
struct pcb {
    int   idx;
    pid_t pid;
    int   cpu_remain;           /* remaining CPU burst (parent's account)  */
    int   io_remain;            /* remaining I/O burst (valid in wait-q)   */
    int   quantum;              /* remaining time quantum                  */
    struct pcb *next;           /* queue link                              */
};
struct queue { struct pcb *head, *tail; };

static struct pcb procs[NPROC];
static struct queue runq, waitq;
static int q_cpu = -1, q_io = -1;
static volatile sig_atomic_t pending_ticks = 0;
static pid_t child_pid[NPROC];

/* ---------- queue helpers ---------- */
static void q_push(struct queue *q, struct pcb *p)
{
    p->next = NULL;
    if (q->tail) q->tail->next = p; else q->head = p;
    q->tail = p;
}
static struct pcb *q_pop(struct queue *q)
{
    struct pcb *p = q->head;
    if (!p) return NULL;
    q->head = p->next;
    if (!q->head) q->tail = NULL;
    p->next = NULL;
    return p;
}
static void q_remove(struct queue *q, struct pcb *target)
{
    struct pcb *prev = NULL, *p = q->head;
    while (p) {
        if (p == target) {
            if (prev) prev->next = p->next; else q->head = p->next;
            if (q->tail == p) q->tail = prev;
            p->next = NULL;
            return;
        }
        prev = p; p = p->next;
    }
}

/* ---------- cleanup ---------- */
static void cleanup(void)
{
    for (int i = 0; i < NPROC; i++)
        if (child_pid[i] > 0) kill(child_pid[i], SIGKILL);
    while (waitpid(-1, NULL, 0) > 0) ;
    if (q_cpu >= 0) msgctl(q_cpu, IPC_RMID, NULL);
    if (q_io  >= 0) msgctl(q_io,  IPC_RMID, NULL);
}
static void on_int(int sig) { (void)sig; cleanup(); _exit(1); }
static void on_alarm(int sig) { (void)sig; pending_ticks++; }

static int rnd(int lo, int hi) { return lo + rand() % (hi - lo + 1); }

/* ---------- child: simulated user process ---------- */
static void child_main(int idx, int cpu_burst, int io_burst)
{
    struct msg_cpu in;
    struct msg_io  out;
    srand((unsigned)(getpid() * 2654435761u) ^ (unsigned)time(NULL));

    sigset_t set; sigemptyset(&set);
    sigprocmask(SIG_SETMASK, &set, NULL);   /* unblock everything */

    for (;;) {                  /* infinite loop of CPU-burst / I/O-burst */
        /* wait for a time slice (one tick) from the "OS" */
        if (msgrcv(q_cpu, &in, sizeof(in) - sizeof(long), idx + 1, 0) < 0) {
            if (errno == EINTR) continue;
            _exit(0);           /* queue removed -> parent is gone */
        }
        if (cpu_burst > 0) cpu_burst--;         /* make progress */

        if (cpu_burst == 0) {                   /* CPU burst finished */
            int next_cpu = rnd(CPU_MIN, CPU_MAX);
            out.mtype          = idx + 1;
            out.pid            = getpid();
            out.io_burst       = io_burst;      /* request I/O */
            out.next_cpu_burst = next_cpu;
            if (msgsnd(q_io, &out, sizeof(out) - sizeof(long), 0) < 0)
                _exit(0);
            cpu_burst = next_cpu;               /* next bursts */
            io_burst  = rnd(IO_MIN, IO_MAX);
        }
    }
}

/* ---------- dump helpers ---------- */
static int fmt_queue_run(char *buf, size_t n, struct queue *q)
{
    int len = snprintf(buf, n, "[");
    for (struct pcb *p = q->head; p && len < (int)n; p = p->next)
        len += snprintf(buf + len, n - len, "%s%d(%d)",
                        p == q->head ? "" : " ", p->pid, p->cpu_remain);
    len += snprintf(buf + len, n - len, "]");
    return len;
}
static int fmt_queue_io(char *buf, size_t n, struct queue *q)
{
    int len = snprintf(buf, n, "[");
    for (struct pcb *p = q->head; p && len < (int)n; p = p->next)
        len += snprintf(buf + len, n - len, "%s%d(io=%d)",
                        p == q->head ? "" : " ", p->pid, p->io_remain);
    len += snprintf(buf + len, n - len, "]");
    return len;
}

static void write_all(int fd, const char *buf, size_t len)
{
    while (len) {
        ssize_t w = write(fd, buf, len);
        if (w < 0) { if (errno == EINTR) continue; perror("write"); return; }
        buf += w; len -= (size_t)w;
    }
}

int main(int argc, char **argv)
{
    long tick_us  = argc > 1 ? atol(argv[1]) : DEF_TICK_US;
    int  max_tick = argc > 2 ? atoi(argv[2]) : DEF_MAXTICK;
    if (tick_us <= 0 || max_tick <= 0) {
        fprintf(stderr, "usage: %s [tick_usec] [max_ticks]\n", argv[0]);
        return 1;
    }
    srand((unsigned)time(NULL));

    /* IPC queues (private, removed on exit) */
    q_cpu = msgget(IPC_PRIVATE, IPC_CREAT | 0600);
    q_io  = msgget(IPC_PRIVATE, IPC_CREAT | 0600);
    if (q_cpu < 0 || q_io < 0) { perror("msgget"); cleanup(); return 1; }

    /* block SIGALRM in the parent; it is only delivered inside sigsuspend */
    sigset_t block, oldmask;
    sigemptyset(&block); sigaddset(&block, SIGALRM);
    sigprocmask(SIG_BLOCK, &block, &oldmask);

    /* create the children */
    for (int i = 0; i < NPROC; i++) {
        int cpu = rnd(CPU_MIN, CPU_MAX), io = rnd(IO_MIN, IO_MAX);
        pid_t pid = fork();
        if (pid < 0) { perror("fork"); cleanup(); return 1; }
        if (pid == 0) {
            for (int j = 0; j < i; j++) child_pid[j] = 0;
            child_main(i, cpu, io);             /* never returns */
            _exit(0);
        }
        child_pid[i]       = pid;
        procs[i].idx       = i;
        procs[i].pid       = pid;
        procs[i].cpu_remain = cpu;
        procs[i].io_remain = 0;
        procs[i].quantum   = 0;
        q_push(&runq, &procs[i]);               /* all start ready */
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_alarm;
    sigaction(SIGALRM, &sa, NULL);
    sa.sa_handler = on_int;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    int fd = open(DUMP_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { perror("open"); cleanup(); return 1; }

    char line[4096], rq[2048], wq[2048];
    int n = snprintf(line, sizeof(line),
        "# RR scheduler: %d processes, quantum=%d ticks, tick=%ld us, ticks=%d\n"
        "# format: (at time t, process pid gets CPU time, remaining CPU-burst)"
        " run-queue dump, I/O-queue dump\n"
        "# run-queue entries: pid(remaining CPU burst), running process excluded;"
        " I/O-queue entries: pid(remaining I/O burst)\n",
        NPROC, QUANTUM, tick_us, max_tick);
    write_all(fd, line, (size_t)n);

    /* start the periodic timer */
    struct itimerval it;
    it.it_interval.tv_sec  = tick_us / 1000000;
    it.it_interval.tv_usec = tick_us % 1000000;
    it.it_value = it.it_interval;
    if (setitimer(ITIMER_REAL, &it, NULL) < 0) { perror("setitimer"); cleanup(); return 1; }

    struct pcb *cur = NULL;
    sigset_t empty; sigemptyset(&empty);

    for (int t = 1; t <= max_tick; t++) {
        while (pending_ticks == 0) sigsuspend(&empty);  /* wait for tick */
        pending_ticks--;

        /* 1. account I/O waiting time; completed I/O -> back to run-queue */
        for (struct pcb *p = waitq.head, *nx; p; p = nx) {
            nx = p->next;
            if (--p->io_remain <= 0) {
                q_remove(&waitq, p);
                p->io_remain = 0;
                q_push(&runq, p);
            }
        }

        /* 2. scheduling decision: quantum expired (or CPU free) */
        if (cur && cur->quantum <= 0) {         /* preempt: to run-queue tail */
            q_push(&runq, cur);
            cur = NULL;
        }
        if (!cur) {
            cur = q_pop(&runq);
            if (cur) cur->quantum = QUANTUM;
        }

        /* 3. give one tick of CPU time to the chosen child via msgq */
        struct pcb *ran = cur;
        int ran_remain = 0;
        int io_req = 0;
        if (cur) {
            struct msg_cpu m = { cur->idx + 1, t };
            if (msgsnd(q_cpu, &m, sizeof(m) - sizeof(long), 0) < 0) {
                perror("msgsnd"); break;
            }
            cur->quantum--;
            cur->cpu_remain--;
            ran_remain = cur->cpu_remain;

            if (cur->cpu_remain <= 0) {
                /* CPU burst finished: child reports its I/O request */
                struct msg_io r;
                ssize_t rc;
                do {
                    rc = msgrcv(q_io, &r, sizeof(r) - sizeof(long),
                                cur->idx + 1, 0);
                } while (rc < 0 && errno == EINTR);
                if (rc < 0) { perror("msgrcv"); break; }
                cur->io_remain  = r.io_burst;
                cur->cpu_remain = r.next_cpu_burst;
                q_push(&waitq, cur);            /* run-queue -> wait-queue */
                cur = NULL;
                io_req = 1;
            }
        }

        /* 4. dump:  (t, pid, remaining CPU burst) run-queue, I/O-queue */
        fmt_queue_run(rq, sizeof(rq), &runq);
        fmt_queue_io (wq, sizeof(wq), &waitq);
        if (ran)
            n = snprintf(line, sizeof(line),
                "(%d, %d, %d)%s run-queue %s, I/O-queue %s\n",
                t, ran->pid, ran_remain, io_req ? " [I/O request]" : "", rq, wq);
        else
            n = snprintf(line, sizeof(line),
                "(%d, idle, -) run-queue %s, I/O-queue %s\n", t, rq, wq);
        write_all(fd, line, (size_t)n);
    }

    /* stop the timer, close the dump, kill children, remove queues */
    struct itimerval off; memset(&off, 0, sizeof(off));
    setitimer(ITIMER_REAL, &off, NULL);
    close(fd);
    cleanup();
    return 0;
}
