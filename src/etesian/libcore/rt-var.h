/* AUTHOR Maximilien M. Cura
 */

#pragma once

namespace ets::core::rt {
    template <typename T>
    struct Option
    {
        union {
            T value;
        };
        bool _has_value{ false };

        Option ()
        { }
        Option (T const &rhs)
            : value{ rhs }
            , _has_value{ true }
        { }
        Option (T &&rhs)
            : value{ std::move (rhs) }
            , _has_value{ true }
        { }
        template <typename U>
        inline Option<T> &operator= (U rhs)
        {
            value = rhs;
            _has_value = true;
            return *this;
        }
        inline T unwrap ()
        {
            return value;
        }

        inline bool has_value ()
        {
            return _has_value;
        }
    };

    template <typename R, typename E>
    struct Result
    {
        union {
            R value;
            E error;
        };
        bool _is_error{ true };
    };

    template <typename T>
    struct Ref
    {
        T &inner;

        inline Ref (T &t)
            : inner{ t }
        { }
        inline Ref (Ref const &other)
            : inner{ other.inner }
        { }
        inline Ref<T> &operator= (Ref const &other)
        {
            inner = other.inner;
            return *this;
        }

        inline T &operator* ()
        {
            return inner;
        }
    };
}

using ::ets::core::rt::Option;
using ::ets::core::rt::Result;