/*
 * PROJECT
 * AUTHOR Maximilien M. Cura
 *
 * FILE ref.h
 * DESC
 */

/*
 * COPYRIGHT (c) 2021 Maximilien M. Cura. ALL RIGHTS RESERVED.
 * USE BY EXPLICIT WRITTEN PERMISSION ONLY.
 */

#ifndef __ROSE_RT_REF
#define __ROSE_RT_REF

namespace rose::rt {
    template <typename T>
    struct Ref
    {
        T &inner;

        Ref (T &x)
            : inner{ x }
        { }
        operator T & ()
        {
            return inner;
        }
    };
}

#endif /* !@__ROSE_RT_REF */
