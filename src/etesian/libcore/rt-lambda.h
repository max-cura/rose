/* AUTHOR Maximilien M. Cura
 */

#pragma once

#include <type_traits>

namespace ets::core::rt {
    template <typename>
    struct ScopedLambda;
    template <typename R, typename... AA>
    struct ScopedLambda<R (AA...)>
    {
        R (*lam_impl)
        (void *, AA...);
        void *lam_arg;

        ScopedLambda (R (*impl) (void *arg, AA...) = nullptr, void *arg = nullptr)
            : lam_impl{ impl }
            , lam_arg{ arg }
        { }

        template <typename... _AA>
        R operator() (_AA &&..._aa) const
        {
            return lam_impl (lam_arg, std::forward<_AA> (_aa)...);
        }
    };

    template <typename F, typename Fr>
    struct ScopedLambdaFunctor;
    template <typename R, typename... AA, typename Fr>
    struct ScopedLambdaFunctor<R (AA...), Fr> : public ScopedLambda<R (AA...)>
    {
        Fr lamf_functor;
        template <typename _Fr>
        ScopedLambdaFunctor (_Fr &&functor)
            : ScopedLambda<R (AA...)> (__impl_fn, this)
            , lamf_functor (std::forward<_Fr> (functor))
        { }
        ScopedLambdaFunctor (const ScopedLambdaFunctor &other)
            : ScopedLambda<R (AA...)> (__impl_fn, this)
            , lamf_functor (other.lamf_functor)
        { }
        ScopedLambdaFunctor (const ScopedLambdaFunctor &&other)
            : ScopedLambda<R (AA...)> (__impl_fn, this)
            , lamf_functor (std::move (other.lamf_functor))
        { }
        ScopedLambdaFunctor &operator= (ScopedLambdaFunctor const &other)
        {
            lamf_functor = other.lamf_functor;
            return *this;
        }
        ScopedLambdaFunctor &operator= (ScopedLambdaFunctor &&other)
        {
            lamf_functor = std::move (other.lamf_functor);
            return *this;
        }

    private:
        static R __impl_fn (void *self_arg, AA... aa)
        {
            return static_cast<ScopedLambdaFunctor *> (self_arg)
                ->lamf_functor (aa...);
        }
    };

    template <typename F, typename Fr>
    struct ScopedLambdaRefFunctor;
    template <typename R, typename... AA, typename Fr>
    struct ScopedLambdaRefFunctor<R (AA...), Fr>
        : public ScopedLambda<R (AA...)>
    {
        const Fr *lamrf_functor;

        ScopedLambdaRefFunctor (Fr const &functor)
            : ScopedLambda<R (AA...)> (__impl_fn, this)
            , lamrf_functor (&functor)
        { }
        ScopedLambdaRefFunctor (ScopedLambdaRefFunctor const &other)
            : ScopedLambda<R (AA...)> (__impl_fn, this)
            , lamrf_functor (other.lamrf_functor)
        { }
        ScopedLambdaRefFunctor (ScopedLambdaRefFunctor &&other)
            : ScopedLambda<R (AA...)> (__impl_fn, this)
            , lamrf_functor (other.lamrf_functor)
        { }
        ScopedLambdaRefFunctor &operator= (ScopedLambdaRefFunctor const &other)
        {
            lamrf_functor = other.lamrf_functor;
            return *this;
        }
        ScopedLambdaRefFunctor &operator= (ScopedLambdaRefFunctor &&other)
        {
            lamrf_functor = other.lamrf_functor;
            return *this;
        }

    private:
        static R __impl_fn (void *self_arg, AA... aa)
        {
            return (*static_cast<ScopedLambdaRefFunctor *> (self_arg)
                         ->lamrf_functor) (aa...);
        }
    };

    template <typename F, typename Fr>
    ScopedLambdaFunctor<F, Fr> scoped_lambda (Fr const &functor)
    {
        return ScopedLambdaFunctor<F, Fr> (functor);
    }
    template <typename F, typename Fr>
    ScopedLambdaFunctor<F, Fr> scoped_lambda (Fr &&functor)
    {
        return ScopedLambdaFunctor<F, Fr> (std::move (functor));
    }
    template <typename F, typename Fr>
    ScopedLambdaRefFunctor<F, Fr> scoped_lambda_ref (Fr const &functor)
    {
        return ScopedLambdaRefFunctor<F, Fr> (functor);
    }
}

using ets::core::rt::scoped_lambda;
using ets::core::rt::scoped_lambda_ref;
using ets::core::rt::ScopedLambda;