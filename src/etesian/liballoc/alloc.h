/* AUTHOR Maximilien M. Cura
 */

#pragma once

#include <stdint.h>
#include <stddef.h>

#include <etesian/libcore/Maybe.h>
#include <etesian/libcore/Result.h>
#include <etesian/liballoc/thread_support.h>

namespace ets::alloc {
    namespace heap_detail {
        int alloc_object (void **objectp, size_t osize);
        int dealloc_object (void *object);
        int create_regional_heap (void **rheapp);
        int add_heap_to_regional_heap (void *rheap, void *heap);
        int free_regional_heap (void *rheap);
        int free_rheaps ();

        extern thread_local thread_support::Local<void *> _ETS_local_heap;
    }
}