/* AUTHOR: Maximilien M. Cura
 */

#pragma once

#include <etesian/libcore/rt-lambda.h>
#include <etesian/libcore/rt-object.h>
#include <etesian/libcore/rt-var.h>

#include <pthread.h>
#include <type_traits>

namespace ets::alloc::thread_support {
    template <typename T,
              bool _DestructInnerOnOwnDestruct = true>
    struct LocalWrapper
    {
        union {
            T inner;
        };
        Option<core::rt::Ref<core::rt::ScopedLambda<void (T &)>>> __injected_destructor;

        LocalWrapper ()
            : inner{}
        { }
        explicit LocalWrapper (T const &x)
            : inner{ x }
        { }
        explicit LocalWrapper (T &&x)
            : inner{ std::move (x) }
        { }
        explicit LocalWrapper (core::rt::ScopedLambda<T ()> const &initializer,
                               core::rt::ScopedLambda<void (T &)> &dtor)
            : inner{ initializer () }
            , __injected_destructor{ core::rt::Ref{ dtor } }
        { }
        explicit LocalWrapper (LocalWrapper &&other)
            : inner{ std::move (other.inner) }
        { }
        ~LocalWrapper ()
        {
            if (__injected_destructor.has_value ()) {
                (*(__injected_destructor.unwrap ()))
                    .
                    operator() (inner);
            }
            if constexpr (_DestructInnerOnOwnDestruct) {
                core::rt::call_destructor (inner);
            }
        }
        inline T &operator* ()
        {
            return inner;
        }
        inline std::add_const_t<T> const &operator* () const
        {
            return inner;
        }
        inline T &operator-> ()
        {
            if constexpr (std::is_pointer_v<T>) {
                return inner;
            } else {
                return &inner;
            }
        }
    };

    struct PThreadMutex
    {
        pthread_mutex_t inner = PTHREAD_MUTEX_INITIALIZER;

        PThreadMutex ();
        ~PThreadMutex ();
        void lock ();
        bool try_lock ();
        void unlock ();
    };
}