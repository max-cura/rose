//#ifndef __linux__
//    #error "rtvalidate-linux only works on linux systems: requires SIGEV_THREAD_ID"
//#endif

#include <condition_variable>
#include <mutex>
#include <set>
#include <functional>

#include <sys/select.h>
#include <sys/types.h>
#if __linux__
    #include <sys/signalfd.h>
#else
    #define SIGRTMIN 42
#endif
#include <unistd.h>
#include <signal.h>
#include <time.h>

typedef void (*perf_sighandler_t) (int, siginfo_t *, void *);
typedef long long perf_timer_interval_t;

// NOT reentrant
struct SignalSetting
{
    sigset_t mask;

    explicit SignalSetting (int signal, bool enable)
    {
        sigemptyset (&mask);
        sigaddset (&mask, SIGRTMIN);
        pthread_sigmask (SIG_BLOCK, &mask, NULL);
    }

    ~SignalSetting (int signal, bool enable)
    {
        pthread_sigmask (SIG_UNBLOCK, &mask, NULL);
    }
};

struct PreemptionFixer
{
    static void with_handler (perf_timer_interval_t, perf_sighandler_t);
};

struct SSNotifier
{
    int sig_fd;
    fd_set sig_fds;

    explicit SSNotifier (int fd)
        : sig_fd{ fd }
    {
        FD_EMPTY (&sig_fds);
        FD_SET (fd, &sig_fds);
    }

    void wait ()
    {
        int r;
        do {
            r = select (1, &sig_fds, NULL, NULL, NULL);
        } while (r == EINTR);
        char buf[1];
        ssize_t read_bytes;
        do {
            read_bytes = read (sig_fd, buf, 1);
        } while (read_bytes == -1 && errno == EINTR);
        if (read_bytes != 1) {
            std::abort ();
        }
    }

    void notify ()
    {
        char buf[1]{ '!' };
        ssize_t written_bytes;
        do {
            write (sig_fd, buf, 1);
        } while (written_bytes == -1 && errno == EINTR);
        if (written_bytes != 1) {
            std::abort ();
        }
    }
};

static void *perf_pthread_start_wrapper (void *);
static thread_local PThread *perf_local_thread_id;

struct PThread
{
    pthread_t pthread;
    std::function<void *(void *)> start_routine;
    void *start_argument;

    sig_atomic_t should_suspend;
    sig_atomic_t in_critical_section;

    SSNotifier did_unsuspend;
    SSNotifier did_critical_section_finish;

    int ktid;

    inline PThread (std::function<void *(void *)> start, void *arg, pthread_attr_t *attr = nullptr)
        : start_routine{ start }
        , start_argument{ arg }
    {
        should_suspend = 0;
        in_critical_section = 0;

        pthread_create (&this->pthread, perf_pthread_start_wrapper, (void *)this);
    }
};

void perf_pthread_mutex_lock_wrapper (pthread_mutex_t *mtx)
{
    pthread_mutex_lock (&mtx->inner);
    perf_local_thread->in_critical_section = true;
}

void perf_thread_mutex_unlock_wrapper (pthread_mutex_t *mtx)
{
    perf_local_thread->in_critical_section = false;
    pthread_mutex_unlock (&mtx->inner);
}

static void perf_pthread_signal_suspender (int signo, siginfo_t *si, void *)
{
    PThread *self = si->si_value.sival_ptr;
    /* do we need to suspend */
    if (self->should_suspend) {
        if (self->in_critical_section) {
            return;
        } else {
            self->did_critical_section_finish.notify ();
            self->did_unsuspend.wait ();
            self->did_critical_section_finish.notify ();
            pth->should_suspend = 0;
        }
    }
}

struct PThreadFixer
{
    std::set<PThread *> pthreads{};
    std::mutex pthread_list_lock{};

    inline static PThreadFixer instance{};

    void pause_all ()
    {
        std::lock_guard g{ pthread_list_lock };
        for (PThread *pth : pthreads) {
            pth->should_suspend = 1;
            pth->did_critical_section_finish.wait ();
        }
    }
    void unpause_all ()
    {
        std::lock_guard g{ pthread_list_lock };
        for (PThread *pth : pthreads) {
            pth->did_unsuspend.wake ();
        }
        for (PThread *pth : pthreads) {
            pth->did_critical_section_finish.wait ();
        }
    }
    void add (PThread *pthread)
    {
        std::lock_guard g{ pthread_list_lock };
        pthreads->insert (pthread);
    }
};

static void *perf_pthread_start_wrapper (void *arg)
{
    PThread *self = (PThread *)arg;
    self->ktid = gettid ();
    perf_local_thread_id = self;

    struct itermspec its;
    struct sigaction sa;
    struct sigevent sev;

    timer_t timerid;

    sa.sa_flags = SA_SIGINFO;
    sa.sa_sigaction = perf_pthread_signal_suspender;
    sigemptyset (&sa.sa_mask);
    if (-1 == sigaction (SIGRTMIN, &sa, NULL)) {
        perror ("couldn't create timer: sigaction failed");
        std::abort ();
    }

    sev.sigev_notify = SIGEV_THREAD_ID;
    sev.sigev_signo = SIGRTMIN;
    sev._sigev_un._tid = self->ktid;
    sev.sigev_value.sival_ptr = self;

    if (-1 == timer_create (CLOCK_REALTIME, &sev, &timerid)) {
        perror ("couldn't create timer: timer_create failed");
        std::abort ();
    }
    its.it_value.tv_sec = 0;
    its.it_value.tv_nsec = PERF_SUSPEND_CHECK_INTERVAL_NS;
    its.it_interval.tv_sec = its.it_value.tv_sec;
    its.it_interval.tv_nsec = its.it_value.tv_nsec;
    if (-1 == timer_settime (timerid, 0, &its, NULL)) {
        perror ("couldn't create timer: timer_settime failed");
        std::abort ();
    }

    return self->start_routine (self->start_argument);
}
