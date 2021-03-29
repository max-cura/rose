/*
 * PROJECT
 * AUTHOR Maximilien M. Cura
 *
 * FILE maybe.h
 * DESC
 */

/*
 * COPYRIGHT (c) 2021 Maximilien M. Cura. ALL RIGHTS RESERVED.
 * USE BY EXPLICIT WRITTEN PERMISSION ONLY.
 */

#ifndef __ROSE_RT_MAYBE
#define __ROSE_RT_MAYBE

#include <rose/rt/ref.h>

namespace rose::rt {
    template <typename T>
    struct Maybe
    {
        union {
            T inner;
        };
        const bool has_value;

        Maybe ()
            : has_value{ false }
        { }
        constexpr Maybe (T const &x)
            : inner{ x }
            , has_value{ true }
        { }
        constexpr Maybe (T &&x)
            : inner{ std::move (x) }
            , has_value{ true }
        { }
        constexpr Maybe (Maybe<T> const &x)
            : has_value{ x.has_value }
        {
            if (has_value) {
                new (&inner) T{ x.inner };
            }
        }
        constexpr Maybe (Maybe<T> &&x) noexcept
            : has_value{ x.has_value }
        {
            if (has_value) {
                new (&inner) T{ std::move (x.inner) };
            }
        }

        constexpr explicit operator bool ()
        {
            return has_value;
        }

        T unwrap ()
        {
            return std::move (inner);
        }

        void force (T x)
        {
            *this = { x };
        }
    };

    template <typename T>
    struct Maybe<T &>
    {
        union {
            Ref<T> inner;
        };
        const bool has_value;

        Maybe ()
            : has_value{ false }
        { }
        constexpr Maybe (T &x)
            : inner{ x }
            , has_value{ true }
        { }
        constexpr Maybe (Maybe<T> const &x)
            : has_value{ x.has_value }
        {
            if (has_value) {
                new (&inner) Ref{ (T &)x.inner };
            }
        }

        constexpr explicit operator bool ()
        {
            return has_value;
        }

        T &unwrap ()
        {
            return (T &)inner;
        }

        void force (T &x)
        {
            *this = { Ref{ x } };
        }
    };

    template <typename T>
    constexpr Maybe<T> maybe (T const &x)
    {
        return { x };
    }
    template <typename T>
    constexpr Maybe<T> maybe (T &&x)
    {
        return { x };
    }
    template <typename T>
    constexpr Maybe<T> maybe ()
    {
        return {};
    }
};


#endif /* !@__ROSE_RT_MAYBE */
