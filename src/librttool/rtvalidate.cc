#include <pthread.h>
#include <set>
#include <mutex>
#include <condition_variable>
#if __linux__
    #include <signal.h>
    #include <time.h>
#endif

namespace rt::perf_wrap {
    typedef void *(*pthread_start_routine_t) (void *);

    struct PThread
    {
        pthread_t pthread;
        std::function<void *(void *)> actual_start_routine;
#if __linux__
        int _linux_ktid{ 0 };
        timer_t _linux_timer{};
#endif
        bool in_critical_section;
        bool csec_spurchk{ false };
        std::mutex critical_section_mutex{};
        std::condition_variable critical_section_end_notifier{};

        void *actual_start_argument;
        bool did_get_suspended{ false };

        PThread (pthread_attr_t *, pthread_start_routine_t, void *);
        //~PThread ();
    };

    struct PThreadManager
    {
        std::set<PThread *> pthreads;
        std::mutex pthread_list_lock;
        std::mutex pause_waiter_lock;
        std::condition_variable did_pause_finish;

        void pause_all ();
        void unpause_all ();
        void add (PThread *);
    };
}

using rt::perf_wrap::PThread;
using rt::perf_wrap::PThreadManager;

static PThreadManager manager;
static pthread_once_t once_create_key = PTHREAD_ONCE_INIT;
static pthread_key_t ref_key;
static std::mutex ref_key_lock;
static void create_ref_key (void)
{
    std::lock_guard g{ ref_key_lock };

    const int r = pthread_key_create (&ref_key, NULL);
    if (r) {
        perror ("couldn't create preemption timer: couldn't register"
                "self-referential key");
        std::terminate ();
    }
}

namespace rt::perf_wrap {
    void __threadcall_suspend (int sig, siginfo_t *si, void *uc)
    {
        struct sigset_t smask;
        sigemptyset (&smask);
        sigaddset (&mask, SIGRTMIN);
        if (-1 == sigprocmask (SIG_SET_MASK, &mask, NULL)) {
            std::terminate ();
        }

        PThread *self;
        {
            std::lock_guard l{ ref_key_lock };
            self = (PThread *)pthread_getspecific (ref_key);
        }
        std::unique_lock<std::mutex> csec_lock{ self->critical_section_mutex };
        if (true == __atomic_load_n (&self->did_get_suspended, __ATOMIC_SEQ_CST)) {
            if (true == __atomic_load_n (&in_critical_section, __ATOMIC_SEQ_CST)) {
                // okay, wait for that to finish
                goto _ret;
            } else {
                self->csec_spurchk = true;
                csec_lock.unlock ();
                std::unique_lock<std::mutex> wait_lock{ manager.pause_waiter_lock };
                self->critical_section_end_notifier.notify_one ();
                manager.did_pause_finish.wait (wait_lock, [&self] -> bool {
                    return __atomic_load_n (&self->did_get_suspended, __ATOMIC_SEQ_CST);
                });
                self->csec_spurchk = false;
                wait_lock.unlock ();
                __atomic_store_n (&self->did_get_suspended, false, __ATOMIC_SEQ_CST);
            }
        }

        if (-1 == sigprocmask (SIG_UNBLOCK, &mask, NULL)) {
            std::terminate ();
        }

    _ret:
        return;
    }
}

void PThreadManager::pause_all ()
{
    std::lock_guard l{ this->pthread_list_lock };
    for (PThread *thread : this->pthreads) {
        std::unique_lock<std::mutex> csec_lock{ thread->critical_section_mutex };

        __atomic_store_n (&thread->did_get_suspended, true, __ATOMIC_SEQ_CST);
        thread->critical_section_end_notifier.wait (csec_lock, [&thread] () -> bool {
            return thread->csec_spurchk;
        });
        thread->csec_spurchk = false;
    }
}

void PThreadManager::unpause_all ()
{
    std::lock_guard l{ this->pthread_list_lock };
    {
        std::lock_guard{ this->pause_waiter_lock };
    }
    for (PThread *thread : this->pthreads) {
        thread->did_get_suspended = false;
    }
    this->did_pause_finish.notify_all ();
}

void PThreadManager::add (PThread *thread)
{
    std::lock_guard l{ this->pthread_list_lock };
    this->pthreads.insert (thread);
}

namespace rt::perf_wrap {

    void wrapped_pthread_mutex_lock (pthread_mutex_t *mtx)
    {
        PThread *self;
        {
            std::lock_guard l{ ref_key_lock };
            self = (PThread *)pthread_getspecific (ref_key);
        }
        pthread_mutex_lock (mtx);
        __atomic_store_n (&self->in_critical_section, true, __ATOMIC_SEQ_CST);
    }

    void wrapped_pthread_mutex_unlock (pthread_mutex_t *mtx)
    {
        PThread *self;
        {
            std::lock_guard l{ ref_key_lock };
            self = (PThread *)pthread_getspecific (ref_key);
        }
        __atomic_store_n (&self->in_critical_section, false, __ATOMIC_SEQ_CST);
        pthread_mutex_unlock (mtx);
    }
}

PThread::PThread (pthread_attr_t *attr, std::function (void *(void *)) start, void *arg)
    : actual_start_routine{ start }
    , actual_start_argument{ arg }
{
    pthread_create (
        &pthread, [] (void *arg) -> void * {
            PThread *self = (PThread *)arg;
#if __linux__
            self->_linux_ktid = gettid ();

            pthread_once (&once_create_key, create_ref_key);
            {
                std::lock_guard l{ ref_key_lock } pthread_setspecific (ref_key, arg);
            }

            struct itimerspec its;
            struct sigaction sact;
            struct sigevent sev;

            sa.sa_flags = SA_SIGINFO;
            sa.sa_sigaction = rt::perf_wrap::__threadcall_suspend;
            sigemptyset (&sa.sa_mask);
            if (-1 == sigaction (SIGRTMIN, &sa, NULL)) {
                dprintf (2, "couldn't establish preemption timer: couldn't create"
                            " signal handler");
                std::terminate ();
            }
            sev.sigev_notify = SIGEV_THREAD_ID;
            sev.segev_signo = SIGRTMIN,
            sev.sigev_value.sival_ptr = &self->_linux_timer;
            if (-1 == timer_create (CLOCK_REALTIME, &sev, &self->_linux_timer)) {
                dprintf (2, "couldn't establish preemption timer: couldn't create"
                            " timer object");
                std::terminate ();
            }
            its.it_value.tv_sec = 0;
            its.it_value.tv_nsec = 50000;
            its.it_interval.tv_sec = its.it_value.tv_sec;
            its.it_interval.tv_nsec = its.it_value.tv_nsec;
            if (-1 == timer_settime (self->_linux_timer, 0, &its, NULL) == -1) {
                dprintf (2, "couldn't establish preemption timer: couldn't set timer");
                std::terminate ();
            }
#endif

            manager.add (self);

            return (self->actual_start_routine) (self->actual_start_routine);
        },
        (void *)this);
}

#define pthread_mutex_lock(...) ::rt::perf_wrap::wrapped_pthread_mutex_lock (__VA_ARGS__)
#define pthread_mutex_unlock(...) ::rt::perf_wrap::wrapped_pthread_mutex_unlock (__VA_ARGS__)

int main (int argc, char **argv)
{
    pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_init (&mut, NULL);

    int var1{ 0 };
    int var2{ 0 };

    PThread a (NULL, [&] {
        for (;;) {
            printf ("A: before critical section\n");
            pthread_mutex_lock (&mut);

            var1++;
            var2++;

            pthread_mutex_unlock (&mut);
            printf ("A: after critical section\n");
            sleep (5);
        }
    });
    PThread b (NULL, [&] {
        for (;;) {
            printf ("A: before critical section\n");
            pthread_mutex_lock (&mut);

            var1--;
            var2--;

            pthread_mutex_unlock (&mut);
            printf ("A: after critical section\n");
            sleep (5);
        }
    });

    for (;;) {
        manager.pause_all ();

        manager.unpause_all ();
    }

    pthread_join (&a.pthread, NULL);
    pthread_join (&b.pthread, NULL);
}
