/* AUTHOR: Maximilien M. Cura
 */

#include <etesian/liballoc/thread_support.h>

using ets::alloc::thread_support::PThreadMutex;

PThreadMutex::PThreadMutex ()
{
    pthread_mutex_init (&inner, NULL);
}

PThreadMutex::~PThreadMutex ()
{
    pthread_mutex_destroy (&inner);
}
void PThreadMutex::lock ()
{
    pthread_mutex_lock (&inner);
}
void PThreadMutex::unlock ()
{
    pthread_mutex_unlock (&inner);
}
bool PThreadMutex::try_lock ()
{
    const int r = pthread_mutex_trylock (&inner);
    return r == 0;
}