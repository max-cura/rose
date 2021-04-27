/* AUTHOR Maximilien M. Cura
 * TIME 5 apr 2021
 */

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <etesian/liballoc/alloc-impl.h>

static uint64_t ets_tid ();
static int ets_on_threadinit_tid ();
static int ets_on_threadkill_tid ();

//! Initialize a block.
//! Thread-Safe: 0
static int ets_block_init (ets_block_t *);
//! Individually free a block.
//! Thread-safe: SINGLE-OWNING
static int ets_block_free (ets_block_t *);
static int ets_block_clean (ets_block_t *);
//! Allocate object from block.
//! Thread-safe: OWNING
static int ets_block_alloc_object (ets_block_t *block, void **object);
//! Deallocate object from block.
//! Thread-safe: 1
static int ets_block_dealloc_object (ets_block_t *block, void *object);
//! Format block to object size.
//! Thread-safe: 0.
static int ets_block_format_to_size (ets_block_t *block, size_t new_size);

static int ets_lkg_init (ets_lkg_t *lkg, size_t lkgi, struct ets_heap *heap);
//! Allocate object form linkage.
//! Thread-safe: OWNING
static int ets_lkg_alloc_object (ets_lkg_t *lkg, struct ets_heap *heap, void **object);
//! Request block from owning heap.
//! Thread-safe: OWNING (LEAF)
static int ets_lkg_req_block_from_heap (struct ets_heap *heap, size_t lkgi, ets_block_t **blockp);
//! Notify linkage that block became empty.
//! Thread-safe: SINGLE
//! Precondition: LL GL
static int ets_lkg_block_did_become_empty (ets_lkg_t *lkg, ets_block_t *block);
//! Notify linkage that block became partially empty.
//! Thread-safe: SINGLE
//! Precondition: LL GL
static int ets_lkg_block_did_become_partially_empty (ets_lkg_t *lkg, ets_block_t *block);
//! Destroy heap
//! Thread-safe: OWNING
static int ets_lkg_evacuate_and_clean (ets_lkg_t *lkg);
static int ets_heap_alloc_object (ets_heap_t *heap, void **object, size_t size);
static int ets_heap_req_block_from_top (ets_heap_t *heap, size_t lkgi, ets_block_t **blockp);
static int ets_heap_req_block_from_heap (ets_heap_t *heap, size_t lkgi, ets_block_t **blockp);
static int ets_heap_req_block_from_ulkg (ets_lkg_t *lkg, size_t osize, ets_block_t **blockp);
static int ets_heap_req_block_from_slkg (ets_lkg_t *lkg, ets_block_t **block);
static int ets_heap_catch (ets_heap_t *heap, ets_block_t *block, size_t lkgi);
static int ets_heap_receive_applicant (ets_heap_t *heap, ets_block_t *block);
static int ets_heap_evacuate_and_clean (ets_heap_t *heap);

static ets_chunk_tracker_t __ets_chunk_tracker = {
    .ct_first = nullptr,
    .ct_access = PTHREAD_MUTEX_INITIALIZER,
};

//! Allocate a new chunk.
//! Thread-safe: 1
static int ets_chunk_alloc (ets_chunk_t **chunk);
//! Bind a chunk to a heap and place it on the tracker.
//! Thread-safe: OWNING
static int ets_chunk_bind (ets_chunk_t *chunk, ets_heap_t *root, ets_chunk_tracker_t *tracker);
/* motivation: ets_chunk_bind followed by ets_heap_req_block_from_ulkg does 0T
 * guarantee sucess (due to preemption).
 */

//! Bind a chunk to a heap, lift out one block, and place it on the tracker.
//! If this function succeeds, it guarantees a usable block in lift, unlike
//! [[ets_chunk_bind]], which does 0T guarantee a usable block upon success,
//! whether due to preemption or error.
//! Thread-safe: OWNING
static int ets_chunk_reserve_and_bind (ets_chunk_t *chunk, ets_block_t **lift, ets_heap_t *root, ets_chunk_tracker_t *tracker);
//! Free all remaining blocks in a chunk, and then the chunk itself.
//! Thread-safe: OWNING.
static int ets_chunk_free (ets_chunk_t *chunk);

#define LIKELY(x) __builtin_expect (!!(x), 1)
#define UNLIKELY(x) __builtin_expect (!!(x), 0)

#define ETS_BLFL_HEAD 0x01
#define ETS_BLFL_IN_THEATRE 0x02
#define ETS_BLFL_ROH 0x04
#define ETS_CHECK_PROMOTION_FAILURES 0
#define ETS_FEATURE_CHUNKS_USE_MEMALIGN 0
#define ETS_FEATURE_CHUNKS_USE_MACH_MAP 0
// Weird version of x!=0 && x!=1
#define ETS_ISERR(x) (!!((x) & ~1))
#define ETS_PAGE_SIZE 0x1000L
#define ETS_TID_ALLOW_LAZY_ALLOC 1
#define ETS_TID_TRY_RECYCLE 0

#define E_OK 0
#define E_FAIL 1
#define E_BL_EMPTY 2
#define E_MAP_FAILED 3
#define E_CH_UNMAP_FAILED 4
#define E_EMPTY 5
#define E_LKG_SPOILED_PROMOTEE 6
#define E_NXLKG 7

#define ETS_TID_NULL 0L
static _Thread_local uint64_t __ETS_tid = ETS_TID_NULL;

#include <stdarg.h>
#include <unistd.h>
#include <errno.h>

#define ETS_LOG_MSGBUF_SIZE 4096
static _Thread_local size_t _ETS_log_context = 0;
static _Thread_local char _ETS_log_msg_buffer[ETS_LOG_MSGBUF_SIZE];
static inline void _ETS_logv (const char *s, va_list vl)
{
    //vdprintf (2, s, vl);
    //return;

    const char *const end = _ETS_log_msg_buffer + ETS_LOG_MSGBUF_SIZE;
    const char *fmt = s;
    char *msg = _ETS_log_msg_buffer;
    char *occur;
    const uint64_t tid_cache = ets_tid ();
    do {
        if (tid_cache) {
            msg += snprintf (msg, end - msg, "<%llX>", tid_cache);
        } else {
            msg += snprintf (msg, end - msg, "<%p>", &__ETS_tid);
        }
        if (msg >= end)
            break;
        if (_ETS_log_context >= (end - msg))
            break;
        memset (msg, '\t', _ETS_log_context);
        msg += _ETS_log_context;
        if (msg == end - 1) {
            *msg = 0;
            break;
        }
        occur = (char *)strchr (fmt, '\n');
        if (!occur) {
            msg += snprintf (msg, end - msg, "%s", fmt);
            snprintf (msg, end - msg, "\n");
            break;
        } else {
            /* TODO: remove strndup */
            char *tmp_fmt_slice = strndup (fmt, occur - fmt + 1);
            if (!tmp_fmt_slice)
                break;
            msg += strlcpy (msg, tmp_fmt_slice, end - msg);
            free (tmp_fmt_slice);
            fmt = occur + 1;
        }
    } while (1);
    //dprintf (2, "Finalized fmtstring: %s\n", _ETS_log_msg_buffer);
    vdprintf (2, _ETS_log_msg_buffer, vl);
}
static inline void _ETS_log_ctxup (const char *s, ...)
{
    ++_ETS_log_context;
    if (!s) {
        return;
    }
    va_list ap;
    va_start (ap, s);
    _ETS_logv (s, ap);
    va_end (ap);
}
static inline void _ETS_log_ctxdown (const char *s, ...)
{
    if (s) {
        va_list ap;
        va_start (ap, s);
        _ETS_logv (s, ap);
        va_end (ap);
    }
    --_ETS_log_context;
}
static inline void _ETS_log_ctx (const char *s, ...)
{
    if (!s) return;
    ++_ETS_log_context;
    va_list ap;
    va_start (ap, s);
    _ETS_logv (s, ap);
    va_end (ap);
    --_ETS_log_context;
}
static inline void _ETS_log (const char *s, ...)
{
    va_list ap;
    va_start (ap, s);
    _ETS_logv (s, ap);
    va_end (ap);
}
#if ETS_ALLOC_USE_LOGS && (!defined(NDEBUG) || !NDEBUG)
    #define VAR(x) x
    #define LOG(...) _ETS_log (__VA_ARGS__);
    #define CTX(...) _ETS_log_ctx (__VA_ARGS__);
    #define CTXUP(...) _ETS_log_ctxup (__VA_ARGS__);
    #define CTXDOWN(...) _ETS_log_ctxdown (__VA_ARGS__);
#else
    #define VAR(x)
    #define LOG(...)
    #define CTX(...)
    #define CTXUP(...)
    #define CTXDOWN(...)
#endif

static int ets_mutex_lock (pthread_mutex_t *mutex)
{
    CTX ("\x1b[31mLOCKING\x1b[0m %p", mutex)
    return pthread_mutex_lock (mutex);
}
static int ets_mutex_unlock (pthread_mutex_t *mutex)
{
    CTX ("\x1b[33mUNLOCKING\x1b[0m %p", mutex)
    return pthread_mutex_unlock (mutex);
}

#ifdef __cplusplus
extern "C"
{
    size_t ets_rlup_sli (size_t lkgi);
    size_t ets_lup_sli (size_t osize);
    bool ets_should_lkg_recv_block (ets_heap_t *heap, ets_lkg_t *lkg);
    bool ets_should_lkg_lift_block (ets_lkg_t *lkg, ets_block_t *block);
}
#else
extern size_t ets_rlup_sli (size_t lkgi);
extern size_t ets_lup_sli (size_t osize);
extern bool ets_should_lkg_recv_block (ets_heap_t *heap, ets_lkg_t *lkg);
extern bool ets_should_lkg_lift_block (ets_lkg_t *lkg, ets_block_t *block);
#endif
/* SECTION: TESTING */

size_t ets_rlup_sli (size_t lkgi)
{
    --lkgi;
    return (16ul << (lkgi >> 1)) + ((lkgi & 1) << ((lkgi >> 1) + 3));
}

//#if !USE_EXTERN_LUPSLI
#if __x86_64__
size_t ets_lup_sli (size_t osize)
{
    __asm volatile(
        "movq %rdi, %r8\n\t"
        "bsrq %rdi, %rax\n\t"
        "movq %rax, %rdx\n\t"
        "shl $1, %rax\n\t"
        "xorq %rsi, %rsi\n\t"
        "btrq %rdx, %rdi\n\t"
        "setnc %sil\n\t"
        "decq %rdx\n\t"
        "btrq %rdx, %rdi\n\t"
        "setc %r9b\n\t"
        "xorq %rcx, %rcx\n\t"
        "testq %rdi, %rdi\n\t"
        "setnz %cl\n\t"
        "andb %r9b, %cl\n\t"
        "subq %rsi, %rax\n\t"
        "addq %rcx, %rax\n\t"
        "subq $6, %rax\n\t"
        "movq $1, %rsi\n\t"
        "cmpq $16, %r8\n\t"
        "cmovleq %rsi, %rax\n\t"
        "pop %rbp\n\t"
        "retq\n\t");
    __builtin_unreachable ();
}
#else
size_t ets_lup_sli (size_t osize)
{
    if (osize <= 16) return 1;
    size_t __nlz;
    size_t __res = (__nlz = 63 - __builtin_clzl (osize))
                       ? (2 * __nlz) - !(~(1 << __nlz) & osize)
                             + ((1 << (__nlz - 1)) & osize
                                && ~(3 << (__nlz - 1)) & osize)
                             - 7
                       : 0;
    return __res + 1;
}
#endif

#define ETS_LKG_LIFT_BOUNDARY_NORMAL_SLKG 16
#define ETS_LKG_LIFT_BOUNDARY_NORMAL_ULKG 24
#define ETS_LKG_LIFT_BOUNDARY_ROOT_SLKG 32
#define ETS_LKG_LIFT_BOUNDARY_ROOT_ULKG 64

bool ets_should_lkg_recv_block (ets_heap_t *heap, ets_lkg_t *lkg)
{
    PRECONDITION ("<LL>");
    if (heap->h_owning_heap == nullptr) {
        return lkg->l_index == 0
                   ? lkg->l_nblocks < ETS_LKG_LIFT_BOUNDARY_ROOT_ULKG
                   : lkg->l_nblocks < ETS_LKG_LIFT_BOUNDARY_ROOT_SLKG;
    }
    return lkg->l_index == 0
               ? lkg->l_nblocks < ETS_LKG_LIFT_BOUNDARY_NORMAL_ULKG
               : lkg->l_nblocks < ETS_LKG_LIFT_BOUNDARY_NORMAL_SLKG;
}

bool ets_should_lkg_lift_block (ets_lkg_t *lkg, ets_block_t *block)
{
    PRECONDITION ("<LL>");
    if (lkg->l_index == 0) {
        fprintf (stderr, "lift event occurred in ulkg.");
        return 0;
    }
    if (lkg->l_owning_heap->h_owning_heap == nullptr) {
        return lkg->l_index == 0
                   ? lkg->l_nblocks >= ETS_LKG_LIFT_BOUNDARY_ROOT_ULKG
                   : lkg->l_nblocks >= ETS_LKG_LIFT_BOUNDARY_ROOT_SLKG;
    }
    return lkg->l_index == 0
               ? lkg->l_nblocks >= ETS_LKG_LIFT_BOUNDARY_NORMAL_ULKG
               : lkg->l_nblocks >= ETS_LKG_LIFT_BOUNDARY_NORMAL_SLKG;
}

/* SECTION: PAGE ALLOCATION */

#if ETS_FEATURE_CHUNKS_USE_MACH_MAP && defined __APPLE__
    #include <mach/vm_statistics.h>
    #include <mach/vm_map.h>
    #include <mach/mach.h>
    #include <mach/vm_types.h>
    #include <mach/mach_vm.h>
#endif
#include <sys/mman.h>
#if ETS_FEATURE_CHUNKS_USE_MEMALIGN
    #include <stdlib.h>
#endif

static int ets_pages_alloc (void **memory, size_t size)
{
#if ETS_FEATURE_CHUNKS_USE_MACH_MAP && defined __APPLE__
    mach_vm_address_t vm_addr;
    kern_return_t kr = mach_vm_map (mach_task_self (),
                                    &vm_addr, size, ~(ETS_PAGE_SIZE),
                                    VM_FLAGS_ANYWHERE | VM_MAKE_TAG (246),
                                    MEMORY_OBJECT_NULL, 0, FALSE,
                                    VM_PROT_DEFAULT, VM_PROT_ALL, VM_INHERIT_DEFAULT);
    if (kr) {
        CTX ("ets_pages_alloc: mach_vm_map failed for size=%p with error code %i (%s)",
             size, kr, mach_error_string (kr));
        /* dead! */
        return E_MAP_FAILED;
    }
    (*memory) = (void *)vm_addr;
#else
    (*memory) = mmap (nullptr, size, PROT_READ | PROT_WRITE,
                      MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (MAP_FAILED == *memory) {
        CTX ("ets_pages_alloc: mmap failed for size=%p with error code %i (%s)",
             size, errno, strerror (errno));
        return E_MAP_FAILED;
    }
#endif
    CTX ("ets_pages_alloc: succeeded for size=%p", size);
    return E_OK;
}

static int ets_pages_alloc_aligned (void **memory, size_t size, size_t align)
{
#if ETS_FEATURE_CHUNKS_USE_MACH_MAP && defined __APPLE__
    mach_vm_address_t vm_addr = nullptr;
    kern_return_t kr = 0;
    kr = mach_vm_map (mach_task_self (),
                      &vm_addr,
                      size,
                      ~(align - 1),
                      VM_FLAGS_ANYWHERE,
                      MEMORY_OBJECT_NULL,
                      0,
                      FALSE,
                      VM_PROT_DEFAULT,
                      VM_PROT_ALL,
                      VM_INHERIT_DEFAULT);
    if (kr) {
        CTX ("ets_pages_alloc_aligned: mach_vm_map failed for size=%p, align=%p"
             " with error code %i (%s)",
             size, align, kr, mach_error_string (kr));
        /* dead! */
        return E_MAP_FAILED;
    } else {
        (*memory) = (void *)vm_addr;
    }
#else
    const size_t mapped_size = (align << 1) - 0x1000;
    /* afaict, MAP_ANONYMOUS is more standard than MAP_ANON */
    void *swath = mmap (nullptr, mapped_size, PROT_READ | PROT_WRITE,
                        MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (swath == MAP_FAILED) {
        CTX ("ets_pages_alloc_aligned: mmap failed for size=%p, align=%p with error code %i (%s)",
             size, align, errno, strerror (errno));
        return E_MAP_FAILED;
    }
    uintptr_t addr = (uintptr_t)swath;
    const uintptr_t offset = addr & (align - 1);
    //printf ("mmap succeeded (size=%p, addr=%p, offset=%p, align=%p)\n", mapped_size, addr, offset, align);
    if (offset) {
        munmap (swath, align - offset);
        addr += (align - offset);
        munmap ((uint8_t *)addr + size, mapped_size - size - (align - offset));
    } else {
        munmap ((uint8_t *)swath + size, mapped_size - size);
    }
    (*memory) = (void *)addr;
#endif
    CTX ("ets_pages_alloc_aligned: succeeded with size=%p, align=%p", size, align);
    return E_OK;
}

static int ets_pages_free (void *memory, size_t size)
{
#if ETS_FEATURE_CHUNKS_USE_MACH_MAP && defined __APPLE__
    kern_return_t kr = mach_vm_deallocate (mach_task_self (), (mach_vm_address_t)memory, size);
    if (kr) {
        CTX ("ets_pages_free: mach_vm_deallocate failed at %p for size=%p with error code %i (%s)",
             memory, size, kr, mach_error_string (kr));
        return E_CH_UNMAP_FAILED;
    }
#else
    int r = munmap (memory, size);
    if (-1 == r) {
        CTX ("ets_pages_free: munmap failed at %p for size=%p with error code %i (%s)",
             memory, size, errno, strerror (errno));
        return E_CH_UNMAP_FAILED;
    }
#endif
    CTX ("ets_pages_free: succeeded at %p for size=%p", memory, size);
    return E_OK;
}

/* SECTION: TID */

struct _ETS_page_vect
{
    size_t pv_size;
    size_t pv_osize;
    size_t pv_nobjs;
    void *pv_pages;
};
#define _ETS_PAGE_VECT_INIT(x)                                            \
    {                                                                     \
        .pv_size = 0, .pv_osize = (x), .pv_nobjs = 0, .pv_pages = nullptr \
    }

static size_t _ets_page_vect_size (struct _ETS_page_vect *pv)
{
    return pv->pv_nobjs;
}

static int _ets_page_vect_push (struct _ETS_page_vect *pv, void *obj)
{
    size_t current_offset = pv->pv_nobjs * pv->pv_osize;
    if (current_offset == pv->pv_size) {
        size_t new_map_size = ETS_PAGE_SIZE;
        if (pv->pv_pages) {
            new_map_size = pv->pv_size << 1;
        }
        void *old_pages = pv->pv_pages;
        pv->pv_pages = mmap (nullptr, new_map_size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED,
                             -1, 0);
        if (MAP_FAILED == pv->pv_pages) {
            fprintf (stderr, "could not grow page_vect: map failed\n");
            abort ();
        }
        memcpy (pv->pv_pages, old_pages, pv->pv_size);
        munmap (old_pages, pv->pv_size);
        pv->pv_size = new_map_size;
    }
    memcpy ((char *)pv->pv_pages + current_offset, obj, pv->pv_osize);
    ++pv->pv_nobjs;

    return E_OK;
}

static int _ets_page_vect_pop (struct _ETS_page_vect *pv, void *obj)
{
    if (!pv->pv_nobjs)
        return E_EMPTY;
    size_t current_offset = (pv->pv_nobjs - 1) * pv->pv_osize;
    --pv->pv_nobjs;
    memcpy (obj, (char *)pv->pv_pages + current_offset, pv->pv_osize);
    if (current_offset & ~(ETS_PAGE_SIZE - 1)) {
        munmap ((char *)pv->pv_pages + current_offset, ETS_PAGE_SIZE);
    }
    return E_OK;
}

static uint64_t __ETS_tid_vcounter = ETS_TID_NULL;
static struct _ETS_page_vect __ETS_tid_recyls = _ETS_PAGE_VECT_INIT (sizeof (uint64_t));
static pthread_mutex_t __ETS_tid_recyaccess = PTHREAD_MUTEX_INITIALIZER;
static pthread_once_t __ETS_tid_recyaccessinit_once = PTHREAD_ONCE_INIT;

#include <stdio.h>

static uint64_t ets_tid_next_monotonic ()
{
    uint64_t next_tid = __atomic_add_fetch (&__ETS_tid_vcounter, 1, __ATOMIC_SEQ_CST);
    if (!next_tid) {
        fprintf (stderr, "cannot assign new thread id: monotonic counter overflow\n");
        abort ();
    }
    return next_tid;
}

#if ETS_TID_TRY_RECYCLE
static void __ETS_init_recyaccess (void)
{
    pthread_mutex_init (&__ETS_tid_recyaccess, nullptr);
}
#endif

static uint64_t ets_tid_next ()
{
#if ETS_TID_TRY_RECYCLE
    pthread_once (&__ETS_tid_recyaccessinit_once, __ETS_init_recyaccess);
    ets_mutex_lock (&__ETS_tid_recyaccess);
    if (_ets_page_vect_size (&__ETS_tid_recyls)) {
        uint64_t tid;
        _ets_page_vect_pop (&__ETS_tid_recyls, &tid);
        return tid;
    }
    ets_mutex_unlock (&__ETS_tid_recyaccess);
#endif
    return ets_tid_next_monotonic ();
}

static uint64_t ets_tid ()
{
#if ETS_TID_ALLOW_LAZY_ALLOC
    if (!__ETS_tid) {
        __ETS_tid = ets_tid_next ();
        if (!__ETS_tid) {
            fprintf (stderr, "cannot assign new thread id\n");
            abort ();
        }
    }
#endif
    return __ETS_tid;
}

static int ets_on_threadinit_tid ()
{
    if (!__ETS_tid) {
        __ETS_tid = ets_tid_next ();
        if (!__ETS_tid) {
            fprintf (stderr, "cannot assign new thread id\n");
            abort ();
        }
    }
    return E_OK;
}

static int ets_on_threadkill_tid ()
{
#if ETS_TID_TRY_RECYCLE
    if (__ETS_tid) {
        _ets_page_vect_push (&__ETS_tid_recyls, &__ETS_tid);
        __ETS_tid = ETS_TID_NULL;
    }
#endif
    return E_OK;
}

/* SECTION: UPSTREAMING */

static int ets_lkg_block_did_become_empty (ets_lkg_t *lkg, ets_block_t *block)
{
    PRECONDITION ("<LL> <GL>");

    CTXUP ("ets_lkg_block_did_become_empty called with lkg=%p, block=%p", lkg, block);

    if (!ets_should_lkg_lift_block (lkg, block)) {
        CTXDOWN ("decided not to lift block (length = %zu)",
                 __atomic_load_n (&lkg->l_nblocks, __ATOMIC_SEQ_CST));
        ets_mutex_unlock (&block->b_access);
        ets_mutex_unlock (&lkg->l_access);
        return E_OK;
    }

    void *heap = ets_get_heap_for_lkg (lkg);
    if (block->b_prev != nullptr) {
        block->b_prev->b_next = block->b_next;
        //block->b_prev = nullptr;
    }
    if (block->b_next != nullptr) {
        block->b_next->b_prev = block->b_prev;
        //block->b_next = nullptr
    }
    /* do not have to worry about l_active */
    __atomic_store_n (&block->b_owning_tid, ETS_TID_NULL, __ATOMIC_SEQ_CST);
    __atomic_and_fetch (&block->b_flags, ~ETS_BLFL_IN_THEATRE, __ATOMIC_SEQ_CST);

    --lkg->l_nblocks;
    ets_mutex_unlock (&lkg->l_access);

    const int r = ets_heap_catch ((ets_heap_t *)heap, block, lkg->l_index);
    CTXDOWN ("ets_heap_catch returned %i", r);
    return r;
}

static int ets_heap_receive_applicant (ets_heap_t *heap, ets_block_t *block)
{
#if ETS_LOG_CHUNK_ENUM
    CTX ("ets_heap_receive_applicant called with heap=%p, block=%p", heap, block);
#endif
    ets_lkg_t *recv_lkg = &heap->h_lkgs[0];
    ets_mutex_lock (&recv_lkg->l_access);
    ets_block_t *head_cache = __atomic_load_n (&recv_lkg->l_active, __ATOMIC_SEQ_CST);
    block->b_next = head_cache;
    if (block->b_next) {
        block->b_prev = head_cache->b_prev;
        block->b_next->b_prev = block;
        if (block->b_prev)
            block->b_prev->b_next = block;
    } else
        block->b_prev = nullptr;
    __atomic_store_n (&block->b_owning_lkg, recv_lkg, __ATOMIC_SEQ_CST);
    __atomic_store_n (&block->b_owning_tid, ETS_TID_NULL, __ATOMIC_SEQ_CST);
    __atomic_store_n (&recv_lkg->l_active, block, __ATOMIC_SEQ_CST);
    ets_mutex_unlock (&recv_lkg->l_access);

    return E_OK;
}

static int ets_lkg_receive_block (ets_lkg_t *recv_lkg, ets_block_t *block)
{
    CTX ("ets_lkg_receive_block called with recv_lkg=%p, block=%p", recv_lkg, block);
    ets_mutex_lock (&recv_lkg->l_access);
    ets_block_t *head_cache = __atomic_load_n (&recv_lkg->l_active, __ATOMIC_SEQ_CST);
    block->b_next = head_cache;
    if (block->b_next) {
        block->b_prev = head_cache->b_prev;
        block->b_next->b_prev = block;
        if (block->b_prev)
            block->b_prev->b_next = block;
    } else
        block->b_prev = nullptr;
    __atomic_store_n (&block->b_owning_lkg, recv_lkg, __ATOMIC_SEQ_CST);
    __atomic_store_n (&block->b_owning_tid, ETS_TID_NULL, __ATOMIC_SEQ_CST);
    __atomic_store_n (&recv_lkg->l_active, block, __ATOMIC_SEQ_CST);
    ets_mutex_unlock (&block->b_access);
    ets_mutex_unlock (&recv_lkg->l_access);

    return E_OK;
}

static int ets_heap_catch (ets_heap_t *heap, ets_block_t *block, size_t lkgi)
{
    PRECONDITION ("<GL> |BADLINK");
    CTXUP ("ets_heap_catch called with heap=%p, block=%p, lkgi=%zu", heap, block, lkgi);

    if (heap == nullptr) {
        /* toplvl */
        ets_block_free (block);
        CTXDOWN ("toplvl free'd block %p", block);
        return E_OK;
    }
    ets_lkg_t *recv_lkg = &heap->h_lkgs[lkgi];
    if (recv_lkg == __atomic_load_n (&block->b_owning_lkg, __ATOMIC_SEQ_CST)) {
        const int r = ets_heap_catch (heap->h_owning_heap, block, lkgi);
        CTXDOWN ("same-heap receive is not permitted on catch (lkg=%p)"
                 "; dispatch to parent returned %i",
                 recv_lkg, r);
        return r;
    }

    if (!__atomic_load_n (&block->b_acnt, __ATOMIC_SEQ_CST)) {
        LOG ("block is empty; promoting to unsized linkage");
        recv_lkg = &heap->h_lkgs[0];
    }
    if (ets_should_lkg_recv_block (heap, recv_lkg)) {
        const int r = ets_lkg_receive_block (recv_lkg, block);
        CTXDOWN ("linkage %p [%zu] accepts block %b, status=%i", recv_lkg, lkgi, block, r);
        return r;
    } else {
        const int r = ets_heap_catch (heap->h_owning_heap, block, lkgi);
        CTXDOWN ("catch failed; dispatch to parent returned %i", r);
        return r;
    }
}

static int ets_lkg_evacuate_and_clean (ets_lkg_t *lkg)
{
    CTXUP ("EVACUATING LINKAGE %p", lkg);
    ets_mutex_lock (&lkg->l_access);
    ets_heap_t *heap = lkg->l_owning_heap;
    ets_block_t *head = __atomic_exchange_n (&lkg->l_active, nullptr, __ATOMIC_SEQ_CST);

    VAR (int evac_block_count = 0;)

    const size_t lkgi = lkg->l_index;
    if (head) {
        ets_block_t *block = head;
        while (block) {
            block = block->b_next;
            ets_mutex_lock (&block->b_access);
            __atomic_and_fetch (&block->b_flags, ~(ETS_BLFL_IN_THEATRE | ETS_BLFL_HEAD), __ATOMIC_SEQ_CST);

            const int r = ets_heap_catch (heap, block, lkgi);
            LOG ("evacuation of block #%i returned with status %i", evac_block_count++, r);
        }
        block = head;
        while (block) {
            ets_mutex_lock (&block->b_access);
            __atomic_and_fetch (&block->b_flags, ~(ETS_BLFL_IN_THEATRE | ETS_BLFL_HEAD), __ATOMIC_SEQ_CST);

            const int r = ets_heap_catch (heap, block, lkgi);
            LOG ("evacuation of block #%i returned with status %i", evac_block_count++, r);
            block = block->b_prev;
        }
    }
    ets_mutex_unlock (&lkg->l_access);
    pthread_mutex_destroy (&lkg->l_access);
    CTXDOWN ("FINISHED EVACUATING LINKAGE");

    return E_OK;
}

static int ets_heap_evacuate_and_clean (ets_heap_t *heap)
{
    CTXUP ("EVACUATING HEAP %p", heap);
    for (size_t i = 0; i < heap->h_nlkgs; ++i) {
        ets_lkg_evacuate_and_clean (&heap->h_lkgs[i]);
    }
    CTXDOWN ("FINISHED EVACUATING HEAP");
    return E_OK;
}

static int ets_lkg_block_did_become_partially_empty (ets_lkg_t *lkg, ets_block_t *block)
{
    PRECONDITION ("<LL> <GL>");
    CTX ("ets_lkg_block_did_become_partially_empty called with lkg=%p, block=%p", lkg, block);

    /* does 0T go above linkage level under any circumstances */

    if (block->b_prev != nullptr)
        block->b_prev->b_next = block->b_next;
    if (block->b_next != nullptr)
        block->b_next->b_prev = block->b_prev;

    ets_block_t *head_cache = __atomic_load_n (&lkg->l_active, __ATOMIC_SEQ_CST);
    block->b_prev = head_cache;
    block->b_next = head_cache->b_next;
    if (block->b_next)
        block->b_next->b_prev = block;
    head_cache->b_next = block;

    __atomic_or_fetch (&block->b_flags, ETS_BLFL_ROH, __ATOMIC_SEQ_CST);
    __atomic_clear (&block->b_flisroh, __ATOMIC_SEQ_CST);

    ets_mutex_unlock (&block->b_access);
    ets_mutex_unlock (&lkg->l_access);

    return E_OK;
}

/* SECTION: CHUNK */

static int ets_block_free (ets_block_t *block)
{
    CTXUP ("ets_block_free called with block=%p");
    ets_chunk_t *chunk = ets_get_chunk_for_block (block);
    const size_t block_no = ets_get_block_no (block);
    const size_t remaining = __atomic_sub_fetch (&chunk->c_nactive, 1, __ATOMIC_SEQ_CST);
    LOG ("determined chunk=%p (block #%zu) with %zu remaining", chunk, block_no, remaining);
    __atomic_and_fetch (&chunk->c_active_mask, ~(1 << ets_get_block_no (block)), __ATOMIC_SEQ_CST);
    ets_mutex_unlock (&block->b_access);
    ets_block_clean (block);
    ets_pages_free (block, ETS_BLOCK_SIZE);

    if (!remaining) {
        const int r = ets_chunk_free (chunk);
        CTXDOWN ("attempt to free chunk %p returned %i", chunk, r);
        return r;
    }
    CTXDOWN ("block %p freed", block);
    return E_OK;
}

static int ets_chunk_bind_impl (ets_chunk_t *chunk, ets_chunk_tracker_t *tracker)
{
    CTXUP ("ets_chunk_bind_impl called with chunk = %p, tracker = %p");
    chunk->c_tracker = tracker;
    ets_mutex_lock (&tracker->ct_access);
    chunk->c_prev = nullptr;
    chunk->c_next = __atomic_load_n (&tracker->ct_first, __ATOMIC_SEQ_CST);
    if (chunk->c_next)
        chunk->c_next->c_prev = chunk;
    __atomic_store_n (&tracker->ct_first, chunk, __ATOMIC_SEQ_CST);
    ets_mutex_unlock (&tracker->ct_access);
    LOG ("tracker updated")

    chunk->c_nactive = 0;
    chunk->c_active_mask = 0;

    for (size_t block_no = 1; block_no < 64; ++block_no) {
        ets_block_t *block = (ets_block_t *)((uint8_t *)chunk + block_no * ETS_BLOCK_SIZE);
        const int r = ets_block_init (block);
        if (E_OK == r) {
            chunk->c_active_mask |= (1 << (block_no - 1));
            ++chunk->c_nactive;
        }
        //LOG ("block init for #%zu returned %i -> nactive=%zu, active_mask=%zx",
        //     block_no, r, chunk->c_nactive, chunk->c_active_mask);
    }
    CTXDOWN ("ets_chunk_bind_impl finishing with %zu/63 active (%zx)",
             chunk->c_nactive, chunk->c_active_mask);

    return E_OK;
}

static int ets_chunk_bind (ets_chunk_t *chunk, ets_heap_t *root, ets_chunk_tracker_t *tracker)
{
    CTXUP ("ets_chunk_bind called with chunk=%p, root=%p, tracker=%p", chunk, root, tracker);
    const int r = ets_chunk_bind_impl (chunk, tracker);
    if (E_OK != r) {
        CTXDOWN ("bind_impl failed with error %i", r);
        return r;
    }

    VAR (int n_bound = 0;)
    for (size_t block_no = 1; block_no < 64; ++block_no) {
        const int filter = (1 << (block_no - 1));
        if (chunk->c_active_mask & filter) {
            ets_block_t *block = (ets_block_t *)((uint8_t *)chunk + block_no * ETS_BLOCK_SIZE);
            ets_heap_receive_applicant (root, block);
            VAR (++n_bound);
        } else {
            LOG ("unbound block #%zu, mask=%zx, filter=%zx",
                 block_no, chunk->c_active_mask, filter);
        }
    }
    CTXDOWN ("dispatched %i/63 blocks", n_bound);
    return E_OK;
}

static int ets_chunk_reserve_and_bind (ets_chunk_t *chunk, ets_block_t **lift, ets_heap_t *root, ets_chunk_tracker_t *tracker)
{
    CTXUP ("ets_chunk_reserve_and_bind called with chunk=%p, root=%p, tracker=%p");
    const int r = ets_chunk_bind_impl (chunk, tracker);
    if (E_OK != r) {
        CTXDOWN ("bind_impl failed with error %i", r);
        return r;
    }

    VAR (int n_bound = 0;)
    bool has_lifted = 0;
    for (size_t block_no = 1; block_no < 64; ++block_no) {
        uint64_t filter = (1 << (block_no - 1));
        if (chunk->c_active_mask & filter) {
            ets_block_t *block = (ets_block_t *)((uint8_t *)chunk + block_no * ETS_BLOCK_SIZE);
            if (!has_lifted) {
                has_lifted = 1;
                (*lift) = block;
            } else {
                ets_heap_receive_applicant (root, block);
            }
            VAR (++n_bound;);
        } else {
            LOG ("unbound block #%zu, mask=%zx, filter=%zx",
                 block_no, chunk->c_active_mask, filter);
        }
    }
    CTXDOWN ("dispatched %i/63 blocks, split %i|%i", n_bound, has_lifted, n_bound - has_lifted);

    return has_lifted ? E_OK : E_FAIL;
}

static int ets_chunk_free (ets_chunk_t *chunk)
{
    CTXUP ("ets_chunk_free called with chunk=%p");
    size_t block_adjust = 62;
    uint64_t mask = chunk->c_active_mask;
    while (chunk->c_active_mask) {
        const size_t nlz = __builtin_clzl (mask);
        const size_t block_no = block_adjust - nlz;
        mask <<= nlz;
        block_adjust -= nlz;
        const size_t span = __builtin_clzl (~mask);
        for (size_t i = 0; i < span; ++i) {
            ets_block_clean ((ets_block_t *)((uint8_t *)chunk + ETS_BLOCK_SIZE * (block_no + i + 1)));
        }
        uint8_t *const locus = (uint8_t *)chunk + ETS_BLOCK_SIZE + ETS_BLOCK_SIZE * block_no;
        const int r = ets_pages_free ((void *)locus, span * ETS_BLOCK_SIZE);
        LOG ("freeing %zu blocks starting at block #%zu returned %i", span, block_no, r);
    }
    ets_chunk_tracker_t *const tracker = chunk->c_tracker;
    ets_mutex_lock (&tracker->ct_access);
    if (chunk == __atomic_load_n (&tracker->ct_first, __ATOMIC_SEQ_CST)) {
        __atomic_store_n (&tracker->ct_first, chunk->c_next, __ATOMIC_SEQ_CST);
        if (chunk->c_next)
            chunk->c_next->c_prev = nullptr;
    } else {
        chunk->c_prev->c_next = chunk->c_next;
        if (chunk->c_next)
            chunk->c_next->c_prev = chunk->c_prev;
    }
    ets_mutex_unlock (&tracker->ct_access);
    LOG ("tracker updated")

    /* free the header */
    const int r = ets_pages_free (chunk, ETS_BLOCK_SIZE);
    CTXDOWN ("freeing header page returned %i", r)
    if (0 != r) return r;

    return E_OK;
}

static int ets_chunk_alloc (ets_chunk_t **chunkp)
{
    CTXUP ("ets_chunk_alloc called with chunkp=%p", chunkp)
    (*chunkp) = nullptr;

    const int r = ets_pages_alloc_aligned ((void **)chunkp, ETS_CHUNK_SIZE, ETS_CHUNK_SIZE);
    if (E_OK != r) {
        CTXDOWN ("ets_pages_alloc_aligned failed with error code %i", r)
        return r;
    }
    (*chunkp)->c_next = nullptr;
    (*chunkp)->c_tracker = nullptr;
    (*chunkp)->c_active_mask = 0;
    (*chunkp)->c_nactive = 0;

    CTXDOWN ("succeeded, chunk=%p", *chunkp)
    return E_OK;
}

/* SECTION: HEAP */

static int ets_heap_alloc_object (ets_heap_t *heap, void **object, size_t osize)
{
    if (!osize) {
        (*object) = nullptr;
        return E_FAIL;
    }
    size_t lkgi = ets_lup_sli (osize);
    CTXUP ("ets_heap_alloc_object called with heap=%p, objectp=%p, osize=%zu | LKGI=%zu",
           heap, object, osize, lkgi)
    /* TODO: large!! */
    if (lkgi >= heap->h_nlkgs) {
        return E_NXLKG;
    }
    const int r = ets_lkg_alloc_object (&heap->h_lkgs[lkgi], heap, object);
    CTXDOWN ("ets_lkg_alloc_object returned %i with object = %p", r, *object);
    return r;
}

static int ets_heap_req_block_from_top (ets_heap_t *heap, size_t lkgi, ets_block_t **blockp)
{
    CTXUP ("ets_heap_req_block_from_top called with heap=%p, lkgi=%zu, blockp=%p",
           heap, lkgi, blockp)
    ets_chunk_t *chunk;
    {
        const int r = ets_chunk_alloc (&chunk);
        if (E_OK != r) {
            CTXDOWN ("ets_chunk_alloc failed with error code %i", r)
            return r;
        }
    }

    ets_block_t *block;
    {
        const int r = ets_chunk_reserve_and_bind (chunk, &block, heap, &__ets_chunk_tracker);
        if (E_OK != r) {
            CTXDOWN ("ets_chunk_reserve_and_bind failed for chunk %p with error code %i",
                     chunk, r);
            return r;
        }
    }
    {
        const int r = ets_block_format_to_size (block, ets_rlup_sli (lkgi));
        if (E_OK != r) {
            CTXDOWN ("ets_block_format_to_size failed for block %p of chunk %p "
                     "with error code %i for size %zu (lkgi=%zu)",
                     block, chunk, r, ets_rlup_sli (lkgi), lkgi)
            return r;
        }
        ets_mutex_lock (&block->b_access);
    }
    (*blockp) = block;
    CTXDOWN ("toplevel successfully reserved block %p", block)
    return E_OK;
}

static int ets_heap_req_block_from_heap (ets_heap_t *heap, size_t lkgi, ets_block_t **blockp)
{
    CTXUP ("ets_heap_req_block_from_heap called with heap=%p, lkgi=%zu, blockp=%p",
           heap, lkgi, blockp)
    int r;
    r = ets_heap_req_block_from_slkg (&heap->h_lkgs[lkgi], blockp);
    if (r == E_OK) {
        CTXDOWN ("ets_heap_req_block_from_slkg succeeded with block=%p", *blockp)
        return E_OK;
    }
    r = ets_heap_req_block_from_ulkg (&heap->h_lkgs[lkgi], ets_rlup_sli (lkgi), blockp);
    if (r == E_OK) {
        CTXDOWN ("ets_heap_req_block_from_ulkg succeeded with block=%p", *blockp)
        return E_OK;
    }
    if (!heap->h_owning_heap) {
        r = ets_heap_req_block_from_top (heap, lkgi, blockp);
        CTXDOWN ("ets_heap_req_block_from_top returned %i; block=%p", r, *blockp)
        return r;
    } else {
        r = ets_heap_req_block_from_heap (heap->h_owning_heap, lkgi, blockp);
        CTXDOWN ("ets_heap_req_block_from_heap returned %i; block=%p", r, *blockp)
        return r;
    }
}

static int ets_heap_req_block_from_ulkg (ets_lkg_t *lkg, size_t osize, ets_block_t **blockp)
{
    CTXUP ("ets_heap_req_block_from_ulkg called with lkg=%p, osize=%zu, blockp=%p",
           lkg, osize, blockp)
    ets_mutex_lock (&lkg->l_access);
    ets_block_t *block_cache = __atomic_load_n (&lkg->l_active, __ATOMIC_SEQ_CST);

#if 0
    ets_block_t *best_match{ nullptr };
    size_t highest_priority{ 0 };
    while (block_cache != nullptr) {
        const size_t priority = __atomic_load_n (&ets_get_chunk_for_block (block_cache)->c_priority);
        if (priority >= highest_priority) {
            best_match = block_cache;
            highest_priority = priority;
        }
        block_cache = block_cache->b_next;
    }
#endif
    if (!block_cache) {
        CTXDOWN ("failed: empty linkage")
        ets_mutex_unlock (&lkg->l_access);
        return E_FAIL;
    }
    ets_mutex_lock (&block_cache->b_access);

    __atomic_store_n (&lkg->l_active, block_cache->b_next, __ATOMIC_SEQ_CST);
    if (block_cache->b_prev)
        block_cache->b_prev->b_next = block_cache->b_next;
    if (block_cache->b_next)
        block_cache->b_next->b_prev = block_cache->b_prev;

    ets_mutex_unlock (&lkg->l_access);
    if (block_cache->b_osize != osize) {
        ets_block_format_to_size (block_cache, osize);
    }
    (*blockp) = block_cache;
    CTXDOWN ("succeeded, block=%p", block_cache)

    return E_OK;
}

static int ets_heap_req_block_from_slkg (ets_lkg_t *lkg, ets_block_t **blockp)
{
    ets_mutex_lock (&lkg->l_access);
    ets_block_t *block_cache = __atomic_load_n (&lkg->l_active, __ATOMIC_SEQ_CST);

#if 0
    ets_block_t *best_match{ nullptr };
    size_t highest_priority{ 0 };
    if (lkg->l_nblocks == 1) {
        ets_mutex_lock (&block_cache->b_access);
        const size_t acnt_cache = __atomic_load_n (&block_cache->b_acnt, __ATOMIC_SEQ_CST);
        ets_mutex_unlock (&block_cache->b_access);
        if (block_cache->b_ocnt == acnt_cache) {
            ets_mutex_unlock (&lkg->l_access);
            return E_FAIL;
        }
    }
#endif
    bool found_match = 0;
    while (block_cache != nullptr) {
        ets_mutex_lock (&block_cache->b_access);
        if (nullptr == __atomic_load_n (&block_cache->b_gfl, __ATOMIC_SEQ_CST)
            && nullptr == __atomic_load_n (&block_cache->b_pfl, __ATOMIC_SEQ_CST)) {
            if (__atomic_load_n (&lkg->l_active, __ATOMIC_SEQ_CST) == block_cache) {
                if (block_cache->b_next != nullptr || block_cache->b_prev == NULL)
                    __atomic_store_n (&lkg->l_active, block_cache->b_next, __ATOMIC_SEQ_CST);
                else
                    __atomic_store_n (&lkg->l_active, block_cache->b_prev, __ATOMIC_SEQ_CST);
            }
            if (block_cache->b_next)
                block_cache->b_next->b_prev = block_cache->b_prev;
            if (block_cache->b_prev)
                block_cache->b_prev->b_next = block_cache->b_next;
            ets_block_t *tmp = block_cache->b_next;
            /* again, cauterize is optional, but makes things easier */
            block_cache->b_next = nullptr;
            block_cache->b_prev = nullptr;
            ets_mutex_unlock (&block_cache->b_access);
            block_cache = tmp;
        } else {
#if 0
            const size_t priority = __atomic_load_n (&ets_get_chunk_for_block (block_cache)->c_priority);
            if (priority >= highest_priority) {
                best_match = block_cache;
                highest_priority = priority;
            }
            block_cache = block_cache->b_next;
#endif
            found_match = 1;
            break;
        }
    }
    if (!found_match) {
        ets_mutex_unlock (&lkg->l_access);
        return E_FAIL;
    }
    ets_mutex_lock (&block_cache->b_access);
    ets_block_t *curr_head = __atomic_load_n (&lkg->l_active, __ATOMIC_SEQ_CST);
    if (curr_head == block_cache) {
        /* if both b_next AND b_prev are nullptr, it'll use b_next which is NULL */
        if (curr_head->b_next != nullptr || curr_head->b_prev == NULL)
            __atomic_store_n (&lkg->l_active, curr_head->b_next, __ATOMIC_SEQ_CST);
        else
            __atomic_store_n (&lkg->l_active, curr_head->b_prev, __ATOMIC_SEQ_CST);
    }
    if (block_cache->b_prev)
        block_cache->b_prev->b_next = block_cache->b_next;
    if (block_cache->b_next)
        block_cache->b_next->b_prev = block_cache->b_prev;

    ets_mutex_unlock (&lkg->l_access);
    (*blockp) = block_cache;

    return E_OK;
}

static int ets_lkg_req_block_from_heap (ets_heap_t *heap, size_t lkgi, ets_block_t **blockp)
{
    /* % .caller LIVE LINKAGE
     * % .callee LIVE HEAP
     */

    ets_lkg_t *ulkg = &heap->h_lkgs[0];
    const int r = ets_heap_req_block_from_ulkg (ulkg, ets_rlup_sli (lkgi), blockp);
    if (r == E_OK) return E_OK;
    if (!heap->h_owning_heap) {
        return ets_heap_req_block_from_top (heap, lkgi, blockp);
    } else {
        return ets_heap_req_block_from_heap (heap->h_owning_heap, lkgi, blockp);
    }
}

/* SECTION: BLOCK */

static int ets_block_init (ets_block_t *block)
{
    ets_opaque_block_t *opaque_block = (ets_opaque_block_t *)block;
    /* TODO: write these out properly */
    memset (block, 0, &opaque_block->b_memory[0] - (uint8_t *)block);

    pthread_mutex_init (&block->b_access, nullptr);

    return E_OK;
}

static int ets_block_clean (ets_block_t *block)
{
    pthread_mutex_destroy (&block->b_access);

    return E_OK;
}

static int ets_block_format_to_size (ets_block_t *block, size_t osize)
{
    PRECONDITION ("block must be locked");
    uint8_t *memory = ((ets_opaque_block_t *)block)->b_memory;
    block->b_pfl = memory;
    __atomic_store_n (&block->b_gfl, nullptr, __ATOMIC_SEQ_CST);
    block->b_osize = osize;
    block->b_ocnt = (ETS_BLOCK_SIZE - sizeof (ets_block_t)) / osize;
    __atomic_store_n (&block->b_flags, 0, __ATOMIC_SEQ_CST);
    __atomic_store_n (&block->b_acnt, 0, __ATOMIC_SEQ_CST);
    CTX ("ets_block_format_to_size called with block=%p, osize=%zu\n"
         " | memory=%p (+%p) | ocnt = %zu",
         block, osize, memory, (memory - (uint8_t *)block), block->b_ocnt)

    size_t i;
    for (i = 0; i < block->b_ocnt; ++i) {
        *(void **)(memory + i * osize) = &memory[(i + 1) * osize];
        //printf ("block_fmt: %p -> %p\n", &memory[i * osize], *(void **)(memory + i * osize));
    }
    *(void **)(memory + (i - 1) * osize) = nullptr;
    //printf ("rewrote %p -> %p\n", &memory[i * osize - osize], *(void **)(memory + i * osize - osize));

    return E_OK;
}

static inline int ets_block_alloc_object_impl (ets_block_t *block, void **object)
{
    (*object) = block->b_pfl;
    block->b_pfl = *(void **)(*object);

    __atomic_add_fetch (&block->b_acnt, 1, __ATOMIC_SEQ_CST);

    return E_OK;
}

static int ets_block_alloc_object (ets_block_t *block, void **object)
{
    CTX ("ets_block_alloc_object called with block=%p, objectp=%p\n"
         " | pfl=%p | acnt=%zu/%zu",
         block, object, block->b_pfl, __atomic_load_n (&block->b_acnt, __ATOMIC_SEQ_CST),
         block->b_ocnt)
    if (block->b_pfl != nullptr) {
        return ets_block_alloc_object_impl (block, object);
    } else {
        ets_mutex_lock (&block->b_access);
        /* not actually atomic, just needed an XCHG instruction */
        block->b_pfl = __atomic_exchange_n (&block->b_gfl, nullptr, __ATOMIC_SEQ_CST);
        ets_mutex_unlock (&block->b_access);
        CTX ("swapped null pfl for gfl; now pfl=%p", block->b_pfl)

        if (LIKELY (block->b_pfl != nullptr)) {
            return ets_block_alloc_object_impl (block, object);
        }
        return E_BL_EMPTY;
    }
}

static int ets_block_dealloc_object (ets_block_t *block, void *object)
{
    CTXUP ("ets_block_dealloc_object called with block=%p, object=%p\n"
           " | acnt = %hu/%hu | flags = %hhu | osize = %hu",
           block, object, __atomic_load_n (&block->b_acnt, __ATOMIC_SEQ_CST),
           block->b_ocnt, block->b_flags, block->b_osize)

    if (ets_tid () == __atomic_load_n (&block->b_owning_tid, __ATOMIC_SEQ_CST)) {
        *(void **)object = block->b_pfl;
        block->b_pfl = object;
    } else {
        ets_mutex_lock (&block->b_access);
        *(void **)object = block->b_gfl;
        block->b_gfl = object;
        ets_mutex_unlock (&block->b_access);
    }

    const size_t acnt_cache = __atomic_sub_fetch (&block->b_acnt, 1, __ATOMIC_SEQ_CST);
    if (0 == acnt_cache) {
        ets_mutex_lock (&block->b_access);
        if (!(ETS_BLFL_HEAD & __atomic_load_n (&block->b_flags, __ATOMIC_SEQ_CST))) {
            if (0 == __atomic_load_n (&block->b_acnt, __ATOMIC_SEQ_CST)) {
                void *pfl_save = block->b_pfl,
                     *gfl_save = block->b_gfl;
                block->b_pfl = nullptr;
                block->b_gfl = nullptr;
                ets_mutex_unlock (&block->b_access);

                ets_lkg_t *const lkg_cache = __atomic_load_n (&block->b_owning_lkg, __ATOMIC_SEQ_CST);
                ets_mutex_lock (&lkg_cache->l_access);
                ets_mutex_lock (&block->b_access);
                block->b_pfl = pfl_save;
                block->b_gfl = gfl_save;

                const int r = ets_lkg_block_did_become_empty (lkg_cache, block);
                CTXDOWN ("ets_lkg_block_did_become_empty returned %i", r)
                return r;
            } else {
                ets_mutex_unlock (&block->b_access);
                CTXDOWN ("couldn't lift: spurious empty on %p", block)
                return E_OK;
            }
        } else {
            ets_mutex_unlock (&block->b_access);
            CTXDOWN ("couldn't lift: head")
            return E_OK;
        }
    } else if (acnt_cache == (block->b_ocnt / 2)) {
        if (ETS_BLFL_ROH & __atomic_load_n (&block->b_flags, __ATOMIC_SEQ_CST)) {
            CTXDOWN ("couldn't right: ROH set")
            return E_OK;
        }
        ets_mutex_lock (&block->b_access);
        if (ETS_BLFL_ROH & __atomic_load_n (&block->b_flags, __ATOMIC_SEQ_CST)) {
            ets_mutex_unlock (&block->b_access);
            CTXDOWN ("couldn't right: spurious ROH")
            return E_OK;
        }
        if (!__atomic_test_and_set (&block->b_flisroh, __ATOMIC_SEQ_CST)) {
            ets_mutex_unlock (&block->b_access);
            CTXDOWN ("couldn't right: being righted")
            return E_OK;
        }
        if (!__atomic_load_n (&block->b_acnt, __ATOMIC_SEQ_CST)) {
            ets_mutex_unlock (&block->b_access);
            __atomic_clear (&block->b_flisroh, __ATOMIC_SEQ_CST);
            CTXDOWN ("couldn't right: zeroed")
            return E_OK;
        }
        /* concurrent accesses possible: allocation path */
        const uint16_t flag_cache = __atomic_load_n (&block->b_flags, __ATOMIC_SEQ_CST);
        if (!(ETS_BLFL_HEAD & flag_cache) && (ETS_BLFL_IN_THEATRE & flag_cache)) {
            /*
             * @TODO: MAJOR ISSUES:
             *  - MULTIPLE PARTIALS
             *  - PARTIAL | ZERO
             */
            if ((block->b_ocnt / 2) >= __atomic_load_n (&block->b_acnt, __ATOMIC_SEQ_CST)) {
                void *pfl_save = block->b_pfl;
                void *gfl_save = block->b_gfl;
                block->b_pfl = nullptr;
                block->b_gfl = nullptr;
                ets_mutex_unlock (&block->b_access);

                /* issue: if a head transfer op is in progress
                 * possible situations:
                 *  - NOT a slide
                 *  - CANT be a pull
                 */
                ets_lkg_t *lkg_cache;
                do {
                    lkg_cache = __atomic_load_n (&block->b_owning_lkg, __ATOMIC_SEQ_CST);
                    /* @TODO: MAJOR ISSUE:
                     *  - COULD THE LINKAGE CHANGE HERE? */
                    ets_mutex_lock (&lkg_cache->l_access);
                    /* once linkage is locked, block's linkage affiliation will *not* change */
                    if (LIKELY (lkg_cache == __atomic_load_n (&block->b_owning_lkg, __ATOMIC_SEQ_CST))) {
                        break;
                    }
                    ets_mutex_unlock (&lkg_cache->l_access);
                } while (1);
                ets_mutex_lock (&block->b_access);
                block->b_pfl = pfl_save;
                block->b_gfl = gfl_save;

                /* clears FLISROH */
                const int r = ets_lkg_block_did_become_partially_empty (lkg_cache, block);
                CTXDOWN ("ets_lkg_block_did_become_partially_empty returned %i", r)
                return r;
            } else {
                ets_mutex_unlock (&block->b_access);
                __atomic_clear (&block->b_flisroh, __ATOMIC_SEQ_CST);
                CTXDOWN ("couldn't right block: spurious acnt increase")
                return E_OK;
            }
        } else {
            ets_mutex_unlock (&block->b_access);
            __atomic_clear (&block->b_flisroh, __ATOMIC_SEQ_CST);
            CTXDOWN ("couldn't right block: head or out-of-theatre")
            return E_OK;
        }
    }

    CTXDOWN ("successful")
    return E_OK;
}

/* SECTION: LINKAGE */

static int ets_lkg_init (ets_lkg_t *lkg, size_t lkgi, ets_heap_t *heap)
{
    lkg->l_index = lkgi;
    lkg->l_owning_heap = heap;
    lkg->l_nblocks = 0;
    lkg->l_active = nullptr;
    pthread_mutex_init (&lkg->l_access, nullptr);

    return E_OK;
}

static int ets_lkg_alloc_object (ets_lkg_t *lkg, ets_heap_t *heap, void **object)
{
    CTXUP ("ets_lkg_alloc_object called with lkg=%p, heap=%p, objectp=%p",
           lkg, heap, object)

    int r;

    ets_block_t *block_cache = __atomic_load_n (&lkg->l_active, __ATOMIC_SEQ_CST);
    if (UNLIKELY (block_cache == nullptr)) {
        LOG ("empty lkg, pulling from upstream...")
        ets_mutex_lock (&lkg->l_access);

        ets_block_t *tmp;
        r = ets_lkg_req_block_from_heap (heap, lkg->l_index, &tmp);
        if (E_OK != r) {
            CTXDOWN ("ets_lkg_req_block_from_heap failed with error code %i", r)
            return r;
        }
        LOG ("got block %p", tmp)
        __atomic_or_fetch (&tmp->b_flags, ETS_BLFL_HEAD | ETS_BLFL_IN_THEATRE, __ATOMIC_SEQ_CST);
        __atomic_and_fetch (&tmp->b_flags, ~ETS_BLFL_ROH, __ATOMIC_SEQ_CST);
        __atomic_store_n (&tmp->b_owning_tid, ets_tid (), __ATOMIC_SEQ_CST);

        __atomic_store_n (&tmp->b_owning_lkg, lkg, __ATOMIC_SEQ_CST);
        tmp->b_next = nullptr;
        tmp->b_prev = nullptr;
        __atomic_store_n (&lkg->l_active, tmp, __ATOMIC_SEQ_CST);

        ets_mutex_unlock (&tmp->b_access);

        ets_mutex_unlock (&lkg->l_access);
#if ETS_CHECK_PROMOTION_FAILURES
        r = ets_block_alloc_object (tmp, object);
        if (r != E_OK) return E_LKG_SPOILED_PROMOTEE;
        return E_OK;
#else
        r = ets_block_alloc_object (tmp, object);
        CTXDOWN ("ets_block_alloc_object returned %i; object=%p", r, *object)
        return r;
#endif
    }

    if (LIKELY (E_OK == ets_block_alloc_object (block_cache, object))) {
        CTXDOWN ("ets_block_alloc_object succeeded (fast path); object=%p", *object)
        return E_OK;
    }

    ets_mutex_lock (&lkg->l_access);
    ets_mutex_lock (&block_cache->b_access);

    if (block_cache->b_next != nullptr) {
        LOG ("attempting slide")
        bool is_slideable = 1;
        for (;;) {
            ets_mutex_lock (&block_cache->b_next->b_access);

            if (nullptr == block_cache->b_next->b_gfl
                && nullptr == block_cache->b_next->b_pfl
            /* double null free lists will ONLY occur naturally in
                     * head or left-of-head blocks */
#if 0
                    /* used to be acnt == 0 but we have to account for partials */
                    && block_cache->b_next->b_ocnt != __atomic_load_n(&block_cache->b_next->b_acnt, __ATOMIC_SEQ_CST)
#endif
            ) {
                ets_block_t *liftee = block_cache->b_next;
                block_cache->b_next = liftee->b_next;
                if (liftee->b_next != nullptr)
                    liftee->b_next->b_prev = block_cache;
                /* cauterize - optional, but makes things easier*/
                liftee->b_next = nullptr;
                liftee->b_prev = nullptr;

                ets_mutex_unlock (&liftee->b_access);
                LOG ("cauterizd block %p, moving on", liftee)
                if (block_cache->b_next == nullptr) {
                    is_slideable = 0;
                    break;
                }
                block_cache = block_cache->b_next;
            } else {
                /* 0T being lifted */
                break;
            }
        }
        if (1 == is_slideable) {
            LOG ("sliding block %p", block_cache->b_next)
            __atomic_and_fetch (&block_cache->b_flags, ~ETS_BLFL_HEAD, __ATOMIC_SEQ_CST);
            __atomic_or_fetch (&block_cache->b_next->b_flags, ETS_BLFL_HEAD | ETS_BLFL_IN_THEATRE, __ATOMIC_SEQ_CST);
            __atomic_and_fetch (&block_cache->b_next->b_flags, ~ETS_BLFL_ROH, __ATOMIC_SEQ_CST);

            block_cache->b_owning_lkg = lkg;

            __atomic_store_n (&lkg->l_active, block_cache->b_next, __ATOMIC_SEQ_CST);

            ets_mutex_unlock (&block_cache->b_next->b_access);
            ets_mutex_unlock (&block_cache->b_access);
            ets_mutex_unlock (&lkg->l_access);

#if ETS_CHECK_PROMOTION_FAILURES
            r = ets_block_alloc_object (block_cache->b_next, object);
            if (r != E_OK) return E_LKG_SPOILED_PROMOTEE;
            return E_OK;
#else
            r = ets_block_alloc_object (block_cache->b_next, object);
            CTXDOWN ("ets_block_alloc_object returned %i; object=%p", r, *object)
            return r;
#endif
        } else {
            /* 0 == is_slideable */
        }
    }

    LOG ("attempting pull")

    ets_block_t *tmp;
    r = ets_lkg_req_block_from_heap (heap, lkg->l_index, &tmp);
    if (ETS_ISERR (r)) {
        CTXDOWN ("ets_lkg_req_block_from_heap failed with error code %i", r)
        return r;
    }
    LOG ("pulled block %p", tmp)

    __atomic_or_fetch (&tmp->b_flags, ETS_BLFL_HEAD | ETS_BLFL_IN_THEATRE, __ATOMIC_SEQ_CST);
    __atomic_and_fetch (&tmp->b_flags, ETS_BLFL_ROH, __ATOMIC_SEQ_CST);
    __atomic_store_n (&tmp->b_owning_tid, ets_tid (), __ATOMIC_SEQ_CST);
    __atomic_store_n (&tmp->b_owning_lkg, lkg, __ATOMIC_SEQ_CST);

    __atomic_and_fetch (&block_cache->b_flags, ~ETS_BLFL_HEAD, __ATOMIC_SEQ_CST);
    tmp->b_prev = block_cache;
    tmp->b_next = block_cache->b_next;
    if (tmp->b_next != nullptr)
        tmp->b_next->b_prev = tmp;
    __atomic_store_n (&lkg->l_active, tmp, __ATOMIC_SEQ_CST);
    ets_mutex_unlock (&tmp->b_access);

    ets_mutex_unlock (&block_cache->b_access);
    ets_mutex_unlock (&lkg->l_access);

#if ETS_CHECK_PROMOTION_FAILURES
    r = ets_block_alloc_object (tmp, object);
    if (r != E_OK) return E_LKG_SPOILED_PROMOTEE;
    return E_OK;
#else
    r = ets_block_alloc_object (tmp, object);
    CTXDOWN ("ets_block_alloc_object returned %i; object=%p", r, *object)
    return r;
#endif
}


/* SECTION: API */

#include <etesian/liballoc/thread_support.h>
#define ETS_HEAP_SIZE (24 + 20 * sizeof (ets_lkg_t))

static thread_local uint8_t _ETS_heap_backing[ETS_HEAP_SIZE];
static auto _ETS_heap_destructor_lambda = scoped_lambda<void (ets_heap_t *&)> (
    [] (ets_heap_t *&heap) -> void {
        ets_heap_evacuate_and_clean (heap);
    });
namespace ets::alloc::heap_detail {
    thread_local ets::alloc::thread_support::LocalWrapper<ets_heap_t *, false>
        _ETS_local_heap (scoped_lambda<ets_heap_t *()> ([] () -> ets_heap_t * {
                             ets_heap_t *heap = (ets_heap_t *)_ETS_heap_backing;
                             heap->h_owning_heap = nullptr;
                             heap->h_nlkgs = 20;
                             for (size_t i = 0; i < heap->h_nlkgs; ++i) {
                                 ets_lkg_init (&heap->h_lkgs[i], i, heap);
                             }
                             return heap;
                         }),
                         _ETS_heap_destructor_lambda);
}

static void *_ETS_last_rheap_block{ nullptr };
static ets::alloc::thread_support::PThreadMutex _ETS_rheaps_access;
static void *_ETS_rheaps_freelist{ nullptr };

namespace ets::alloc::heap_detail {
    int create_regional_heap (void **rheapp)
    {
        struct _OpaqueHeap
        {
            uint8_t _0[ETS_HEAP_SIZE];
        };
        _ETS_rheaps_access.lock ();
        if (!_ETS_rheaps_freelist) {
            void *new_rheap_block;
            ets_pages_alloc (&new_rheap_block, 0x4000);
            *(void **)new_rheap_block = _ETS_last_rheap_block;
            _OpaqueHeap *ophps = (_OpaqueHeap *)((void **)new_rheap_block + 1);
            const size_t cnt = (0x4000 / sizeof (_OpaqueHeap)) - 1;
            size_t i;
            for (i = 0; i < cnt - 1; ++i) {
                *((void **)&ophps[i]) = ophps + i + 1;
            }
            *((void **)&ophps[i]) = nullptr;
            _ETS_last_rheap_block = new_rheap_block;
        }

        (*rheapp) = _ETS_rheaps_freelist;
        _ETS_rheaps_freelist = *((void **)_ETS_rheaps_freelist);
        _ETS_rheaps_access.unlock ();

        return E_OK;
    }

    int free_regional_heap (void *rheap)
    {
        memset (rheap, 0, ETS_HEAP_SIZE);
        _ETS_rheaps_access.lock ();
        *(void **)rheap = _ETS_rheaps_freelist;
        _ETS_rheaps_freelist = rheap;
        _ETS_rheaps_access.unlock ();
        return E_OK;
    }

    int free_rheaps ()
    {
        _ETS_rheaps_access.lock ();
        void *iter = _ETS_last_rheap_block;
        while (iter) {
            void *next_iter = *(void **)iter;
            ets_pages_free (iter, 0x4000);
            iter = next_iter;
        }
        _ETS_rheaps_access.unlock ();
        return E_OK;
    }

    int add_heap_to_regional_heap (void *rheap, void *heap)
    {
        ((ets_heap_t *)heap)->h_owning_heap = (ets_heap_t *)rheap;
        __atomic_add_fetch (&((ets_heap_t *)rheap)->h_owned_heaps, 1, __ATOMIC_SEQ_CST);
        return E_OK;
    }

    int dealloc_object (void *object)
    {
        if (!object)
            return E_FAIL;
        ets_block_t *block = ets_get_block_for_object (object);
        return ets_block_dealloc_object (block, object);
    }
    int alloc_object (void **objectp, size_t osize)
    {
        return ::ets_heap_alloc_object (*_ETS_local_heap, objectp, osize);
    }
}
