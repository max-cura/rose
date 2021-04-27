/* AUTHOR Maximilien M. Cura
 */

#pragma once

#include <type_traits>

namespace ets::core::rt {
    namespace detail {
        template <typename T>
        void call_ptr_destructor (T t)
        {
            t->~T ();
        }
        template <typename T>
        void call_rref_destructor (T &&t)
        {
            t.~T ();
        }
    }

    template <typename T>
    void call_destructor (T obj)
    {
        if constexpr (std::is_pointer_v<std::remove_reference_t<T>>) {
            detail::call_ptr_destructor<std::remove_cv_t<T>> (
                (std::remove_cv_t<T>)obj);
        } else {
            detail::call_rref_destructor<std::remove_cvref_t<T>> (
                (std::remove_cv_t<T>)obj);
        }
    }
}