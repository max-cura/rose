/* AUTHOR Maximilien M. Cura
 * TIME 5 apr 2021
 */

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

struct ets_lkg;

static uint64_t ets_tid ();
static int ets_on_threadinit_tid ();
static int ets_on_threadkill_tid ();

#define PRECONDITION(s)

#define ETS_BLOCK_SIZE 0x4000L

//! Block of memory in a chunk.
typedef struct ets_block
{
    void *b_pfl, *b_gfl;

    uint16_t b_flags;
    uint16_t b_ocnt;
    uint16_t b_acnt;
    uint16_t b_osize;

    struct ets_block *b_prev, *b_next;
    struct ets_lkg *b_owning_lkg;
    uint64_t b_owning_tid;

    pthread_mutex_t b_access;
} ets_block_t;

typedef struct ets_opaque_block
{
    void *b_pfl, *b_gfl;

    uint16_t b_flags;
    uint16_t b_ocnt;
    uint16_t b_acnt;
    uint16_t b_osize;

    struct ets_block *b_prev, *b_next;
    struct ets_lkg *b_owning_lkg;
    uint64_t b_owning_tid;

    pthread_mutex_t b_gaccess;
    uint8_t b_memory[ETS_BLOCK_SIZE - sizeof (ets_block_t)];
} ets_opaque_block_t;

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
//! Get the block that owns the object at the specified memory location.
//! Thread-safe:1
static inline ets_block_t *ets_get_block_for_object (void *object)
{
    return (void *)((uintptr_t)object & ~(ETS_BLOCK_SIZE - 1));
}

struct ets_heap;

//! Linked list structure with special rules:
//!  1. blocks to the right of head always have a significant free space
//!  2. if the number of blocks to the right of head grows past a certain point
//!         the blocks will be upstreamed
//! Sized heaps only:
//!  3. blocks to the left of head do not have significant free space
//!  4. blocks will only ever be added to the right of the head when
//!         blocks from the left are emptied out past a certain point
//!         -or- when blocks from a downstream heap are evacuating
//!  5. blocks will only ever be added to the left of the head when the head
//!         becomes full -or- when blocks from a downstream heap are evacuating
typedef struct ets_lkg
{
    struct ets_heap *l_owning_heap;
    ets_block_t *l_active;
    size_t l_index;
    size_t l_nblocks;
    pthread_mutex_t l_access;
} ets_lkg_t;

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

//! Either local or regional; global is hardcoded as a NULL value in
//! `h_owning_heap`.
//! 0TE: IF ADDING OR SUBTRACTING MEMBERS, REMEMBER TO MODIFY ets_get_heap_for_lkg
typedef struct ets_heap
{
    struct ets_heap *h_owning_heap;
    size_t h_nlkgs;
    ets_lkg_t h_lkgs[];
} ets_heap_t;

struct ets_arena;

static int ets_heap_alloc_object (ets_heap_t *heap, void **object, size_t size);
static int ets_heap_req_block_from_top (ets_heap_t *heap, size_t lkgi, ets_block_t **blockp);
static int ets_heap_req_block_from_heap (ets_heap_t *heap, size_t lkgi, ets_block_t **blockp);
static int ets_heap_req_block_from_ulkg (ets_lkg_t *lkg, size_t osize, ets_block_t **blockp);
static int ets_heap_req_block_from_slkg (ets_lkg_t *lkg, ets_block_t **block);
static int ets_heap_catch (ets_heap_t *heap, ets_block_t *block, size_t lkgi);
#if 0
static int ets_heap_receive_evacuating_block_from_lkg (ets_heap_t *heap, ets_block_t *block, ets_lkg_t *ctx);
static int ets_heap_receive_evacuating_block_from_heap (ets_heap_t *heap, ets_block_t *block, ets_heap_t *ctx);
#endif
static int ets_heap_receive_applicant (ets_heap_t *heap, ets_block_t *block);
static int ets_heap_evacuate_and_clean (ets_heap_t *heap);
static inline ets_heap_t *ets_get_heap_for_lkg (ets_lkg_t *lkg)
{
    return (ets_heap_t *)((uint8_t *)(lkg - lkg->l_index) - sizeof (ets_heap_t *) - sizeof (size_t));
}

//! Allocation pressure hints (currently unused)
typedef enum ets_chunktype {
    ETS_CHUNKTYPE_NORMAL = 0x00,
    ETS_CHUNKTYPE_HOT = 0x01,
    ETS_CHUNKTYPE_COLD = 0x02,
} ets_chunktype_t;
#define ETS_CHUNKTYPE_MASK 0x03

struct ets_chunk_tracker;

#define ETS_CHUNK_SIZE 0x100000L
//! Large memory chunk
typedef struct ets_chunk
{
    struct ets_chunk *c_next, *c_prev;
    struct ets_chunk_tracker *c_tracker;
    int64_t c_flags;
    size_t c_nactive;
    uint64_t c_active_mask;
} ets_chunk_t;
typedef struct ets_chunk_tracker
{
    ets_chunk_t *ct_first;
    pthread_mutex_t ct_access;
} ets_chunk_tracker_t;

static ets_chunk_tracker_t __ets_chunk_tracker;

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
static inline ets_chunk_t *ets_get_chunk_for_block (ets_block_t *block)
{
    return (void *)((uintptr_t)block & ~(ETS_CHUNK_SIZE - 1));
}
static inline size_t ets_get_block_no (ets_block_t *block)
{
    return (((uintptr_t)block & ~(ETS_CHUNK_SIZE - 1)) / ETS_BLOCK_SIZE) - 1;
}

#define LIKELY(x) __builtin_expect (!(x), 0)
#define UNLIKELY(x) __builtin_expect ((x), 0)

#define ETS_BLFL_HEAD 0x01
#define ETS_BLFL_IN_THEATRE 0x02
#define ETS_CHECK_PROMOTION_FAILURES 0
#define ETS_FEATURE_CHUNKS_USE_MEMALIGN 0
// Weird version of x!=0 && x!=1
#define ETS_ISERR(x) (!!((x) & ~1))
#define ETS_PAGE_SIZE 0x1000L
#define ETS_TID_ALLOW_LAZY_ALLOC 1
#define ETS_TID_TRY_RECYCLE 1

#define E_OK 0
#define E_FAIL 1
#define E_BL_EMPTY 2
#define E_MAP_FAILED 3
#define E_CH_UNMAP_FAILED 4
#define E_EMPTY 5
#define E_LKG_SPOILED_PROMOTEE 6

static size_t ets_rlup_sli (size_t lkgi);
static _Bool ets_should_lkg_recv_block (ets_heap_t *heap, ets_lkg_t *lkg);
static _Bool ets_should_lkg_lift_block (ets_lkg_t *lkg, ets_block_t *block);

/* SECTION: TESTING */

int main (int argc, char **argv)
{
    return 0;
}

/* SECTION: PAGE ALLOCATION */

#if defined __APPLE__
    #include <mach/vm_statistics.h>
    #include <mach/vm_map.h>
    #include <mach/mach.h>
    #include <mach/vm_types.h>
    #include <mach/mach_vm.h>
//#include </Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk/usr/include/mach/vm_types.h>
#endif
#include <sys/mman.h>
#if ETS_FEATURE_CHUNKS_USE_MEMALIGN
    #include <stdlib.h>
#endif

static int ets_pages_alloc (void **memory, size_t size)
{
#if defined __APPLE__
    mach_vm_address_t vm_addr;
    kern_return_t kr = mach_vm_map (mach_task_self (),
                                    &vm_addr, size, ~ETS_PAGE_SIZE,
                                    VM_FLAGS_ANYWHERE | VM_MAKE_TAG (246),
                                    MEMORY_OBJECT_NULL, 0, FALSE,
                                    VM_PROT_DEFAULT, VM_PROT_ALL, VM_INHERIT_DEFAULT);
    if (kr) {
        /* dead! */
        return E_MAP_FAILED;
    }
    (*memory) = (void *)vm_addr;
#else
    (*memory) = mmap (NULL, size, PROT_READ | PROT_WRITE,
                      MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (MAP_FAILED == *memory) {
        return E_MAP_FAILED;
    }
#endif
    return E_OK;
}

static int ets_pages_alloc_aligned (void **memory, size_t size, size_t align)
{
#if defined __APPLE__
    mach_vm_address_t vm_addr;
    kern_return_t kr = mach_vm_map (mach_task_self (),
                                    &vm_addr,
                                    size,
                                    ~align,
                                    VM_FLAGS_ANYWHERE
                                        | VM_MAKE_TAG (246),
                                    MEMORY_OBJECT_NULL,
                                    0,
                                    FALSE,
                                    VM_PROT_DEFAULT,
                                    VM_PROT_ALL,
                                    VM_INHERIT_DEFAULT);
    if (kr) {
        /* dead! */
        return E_MAP_FAILED;
    }
    (*memory) = (void *)vm_addr;
#else
    const size_t mapped_size = (align << 1) - 0x1000;
    /* afaict, MAP_ANONYMOUS is more standard than MAP_ANON */
    void *swath = mmap (NULL, mapped_size, PROT_READ | PROT_WRITE,
                        MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (swath == MAP_FAILED) {
        return E_MAP_FAILED;
    }
    uintptr_t addr = swath;
    const uintptr_t offset = addr & ~(align - 1);
    if (offset) {
        munmap (swath, alignment - offset);
        addr += (alignment - offset);
        munmap ((uint8_t *)addr + size, mapped_size - size - (alignment - offset));
    } else {
        munmap ((uint8_t *)swath + size, mapped_size - size);
    }
    (*memory) = addr;
#endif
    return E_OK;
}

static int ets_pages_free (void *memory, size_t size)
{
#if defined __APPLE__
    kern_return_t kr = mach_vm_deallocate (mach_task_self (), (mach_vm_address_t)memory, size);
    if (kr) {
        return E_CH_UNMAP_FAILED;
    }
    return E_OK;
#else
    munmap (size, size);
    return E_OK;
#endif
}

/* SECTION: TID */

#define ETS_TID_NULL 0L

struct _ETS_page_vect
{
    size_t pv_size;
    size_t pv_osize;
    size_t pv_nobjs;
    void *pv_pages;
};
#define _ETS_PAGE_VECT_INIT(x)                                         \
    {                                                                  \
        .pv_size = 0, .pv_osize = (x), .pv_nobjs = 0, .pv_pages = NULL \
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
        pv->pv_pages = mmap (NULL, new_map_size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED,
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

static _Thread_local uint64_t __ETS_tid = ETS_TID_NULL;
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
    pthread_mutex_init (&__ETS_tid_recyaccess, NULL);
}
#endif

static uint64_t ets_tid_next ()
{
#if ETS_TID_TRY_RECYCLE
    pthread_once (&__ETS_tid_recyaccessinit_once, __ETS_init_recyaccess);
    pthread_mutex_lock (&__ETS_tid_recyaccess);
    if (_ets_page_vect_size (&__ETS_tid_recyls)) {
        uint64_t tid;
        _ets_page_vect_pop (&__ETS_tid_recyls, &tid);
        return tid;
    }
    pthread_mutex_unlock (&__ETS_tid_recyaccess);
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

    if (!ets_should_lkg_lift_block (lkg, block)) {
        return E_OK;
    }

    void *heap = ets_get_heap_for_lkg (lkg);
    if (block->b_prev != NULL) {
        block->b_prev->b_next = block->b_next;
        //block->b_prev = NULL;
    }
    if (block->b_next != NULL) {
        block->b_next->b_prev = block->b_prev;
        //block->b_next = NULL
    }
    /* do not have to worry about l_active */
    __atomic_store_n (&block->b_owning_tid, ETS_TID_NULL, __ATOMIC_SEQ_CST);
    __atomic_and_fetch (&block->b_flags, ~ETS_BLFL_IN_THEATRE, __ATOMIC_SEQ_CST);

    --lkg->l_nblocks;
    pthread_mutex_unlock (&lkg->l_access);

    return ets_heap_catch (heap, block, lkg->l_index);
}

static int ets_heap_receive_applicant (ets_heap_t *heap, ets_block_t *block)
{
    ets_lkg_t *recv_lkg = &heap->h_lkgs[0];
    pthread_mutex_lock (&recv_lkg->l_access);
    ets_block_t *head_cache = __atomic_load_n (&recv_lkg->l_active, __ATOMIC_SEQ_CST);
    block->b_next = head_cache;
    if (block->b_next) {
        block->b_prev = head_cache->b_prev;
        block->b_next->b_prev = block;
        if (block->b_prev)
            block->b_prev->b_next = block;
    } else
        block->b_prev = NULL;
    __atomic_store_n (&block->b_owning_lkg, recv_lkg, __ATOMIC_SEQ_CST);
    __atomic_store_n (&block->b_owning_tid, ETS_TID_NULL, __ATOMIC_SEQ_CST);
    __atomic_store_n (&recv_lkg->l_active, block, __ATOMIC_SEQ_CST);
    pthread_mutex_unlock (&recv_lkg->l_access);

    return E_OK;
}

static int ets_lkg_receive_block (ets_lkg_t *recv_lkg, ets_block_t *block)
{
    pthread_mutex_lock (&recv_lkg->l_access);
    ets_block_t *head_cache = __atomic_load_n (&recv_lkg->l_active, __ATOMIC_SEQ_CST);
    block->b_next = head_cache;
    if (block->b_next) {
        block->b_prev = head_cache->b_prev;
        block->b_next->b_prev = block;
        if (block->b_prev)
            block->b_prev->b_next = block;
    } else
        block->b_prev = NULL;
    __atomic_store_n (&block->b_owning_lkg, recv_lkg, __ATOMIC_SEQ_CST);
    __atomic_store_n (&block->b_owning_tid, ETS_TID_NULL, __ATOMIC_SEQ_CST);
    __atomic_store_n (&recv_lkg->l_active, block, __ATOMIC_SEQ_CST);
    pthread_mutex_unlock (&block->b_access);
    pthread_mutex_unlock (&recv_lkg->l_access);

    return E_OK;
}

static int ets_heap_catch (ets_heap_t *heap, ets_block_t *block, size_t lkgi)
{
    PRECONDITION ("<GL> |BADLINK");

    if (heap == NULL) {
        /* toplvl */
        ets_block_free (block);
    }
    ets_lkg_t *recv_lkg = &heap->h_lkgs[lkgi];
    if (recv_lkg == __atomic_load_n (&block->b_owning_lkg, __ATOMIC_SEQ_CST))
        ets_heap_catch (heap->h_owning_heap, block, lkgi);

    if (!__atomic_load_n (&block->b_acnt, __ATOMIC_SEQ_CST)) {
        recv_lkg = &heap->h_lkgs[0];
    }
    if (ets_should_lkg_recv_block (heap, recv_lkg)) {
        ets_lkg_receive_block (recv_lkg, block);
        return E_OK;
    } else {
        return ets_heap_catch (heap->h_owning_heap, block, lkgi);
    }
}

static int ets_lkg_evacuate_and_clean (ets_lkg_t *lkg)
{
    pthread_mutex_lock (&lkg->l_access);
    ets_heap_t *heap = lkg->l_owning_heap;
    ets_block_t *head = __atomic_exchange_n (&lkg->l_active, NULL, __ATOMIC_SEQ_CST);

    const size_t lkgi = lkg->l_index;
    if (head) {
        ets_block_t *block = head;
        while (block) {
            block = block->b_next;
            pthread_mutex_lock (&block->b_access);
            __atomic_and_fetch (&block->b_flags, ~(ETS_BLFL_IN_THEATRE | ETS_BLFL_HEAD), __ATOMIC_SEQ_CST);

            ets_heap_catch (heap, block, lkgi);
        }
        block = head;
        while (block) {
            pthread_mutex_lock (&block->b_access);
            __atomic_and_fetch (&block->b_flags, ~(ETS_BLFL_IN_THEATRE | ETS_BLFL_HEAD), __ATOMIC_SEQ_CST);

            ets_heap_catch (heap, block, lkgi);
            block = block->b_prev;
        }
    }
    pthread_mutex_unlock (&lkg->l_access);
    pthread_mutex_destroy (&lkg->l_access);

    return E_OK;
}

static int ets_heap_evacuate_and_clean (ets_heap_t *heap)
{
    for (size_t i = 0; i < heap->h_nlkgs; ++i) {
        ets_lkg_evacuate_and_clean (&heap->h_lkgs[i]);
    }
    return E_OK;
}

static int ets_lkg_block_did_become_partially_empty (ets_lkg_t *lkg, ets_block_t *block)
{
    PRECONDITION ("<LL> <GL>");

    /* does 0T go above linkage level under any circumstances */

    if (block->b_prev != NULL)
        block->b_prev->b_next = block->b_next;
    if (block->b_next != NULL)
        block->b_next->b_prev = block->b_prev;

    ets_block_t *head_cache = __atomic_load_n (&lkg->l_active, __ATOMIC_SEQ_CST);
    block->b_prev = head_cache;
    block->b_next = head_cache->b_next;
    if (block->b_next)
        block->b_next->b_prev = block;
    head_cache->b_next = block;

    pthread_mutex_unlock (&block->b_access);
    pthread_mutex_unlock (&lkg->l_access);

    return E_OK;
}

/* SECTION: CHUNK */

static int ets_block_free (ets_block_t *block)
{
    ets_chunk_t *chunk = ets_get_chunk_for_block (block);
    const size_t block_no = ets_get_block_no (block);
    const size_t remaining = __atomic_sub_fetch (&chunk->c_nactive, 1, __ATOMIC_SEQ_CST);
    __atomic_and_fetch (&chunk->c_active_mask, ~(1 << ets_get_block_no (block)), __ATOMIC_SEQ_CST);
    pthread_mutex_unlock (&block->b_access);
    ets_block_clean (block);
    ets_pages_free (block, ETS_BLOCK_SIZE);

    if (!remaining) {
        return ets_chunk_free (chunk);
    }
    return E_OK;
}

static int ets_chunk_bind_impl (ets_chunk_t *chunk, ets_chunk_tracker_t *tracker)
{
    chunk->c_tracker = tracker;
    pthread_mutex_lock (&tracker->ct_access);
    chunk->c_prev = NULL;
    chunk->c_next = __atomic_load_n (&tracker->ct_first, __ATOMIC_SEQ_CST);
    if (chunk->c_next)
        chunk->c_next->c_prev = chunk;
    __atomic_store_n (&tracker->ct_first, chunk, __ATOMIC_SEQ_CST);
    pthread_mutex_unlock (&tracker->ct_access);

    chunk->c_nactive = 0;
    chunk->c_active_mask = 0;

    for (size_t block_no = 1; block_no < 64; ++block_no) {
        ets_block_t *block = (ets_block_t *)((uint8_t *)chunk + block_no * ETS_BLOCK_SIZE);
        if (E_OK == ets_block_init (block)) {
            chunk->c_active_mask |= 1 << (block_no - 1);
            ++chunk->c_nactive;
        }
    }

    return E_OK;
}

static int ets_chunk_bind (ets_chunk_t *chunk, ets_heap_t *root, ets_chunk_tracker_t *tracker)
{
    const int r = ets_chunk_bind_impl (chunk, tracker);
    if (E_OK != r) return r;

    for (size_t block_no = 1; block_no < 64; ++block_no) {
        if (LIKELY (chunk->c_active_mask & (1 << (block_no - 1)))) {
            ets_block_t *block = (ets_block_t *)((uint8_t *)chunk + block_no * ETS_BLOCK_SIZE);
            ets_heap_receive_applicant (root, block);
        }
    }
    return E_OK;
}

static int ets_chunk_reserve_and_bind (ets_chunk_t *chunk, ets_block_t **lift, ets_heap_t *root, ets_chunk_tracker_t *tracker)
{
    const int r = ets_chunk_bind_impl (chunk, tracker);
    if (E_OK != r) return r;

    _Bool has_lifted = 0;
    for (size_t block_no = 1; block_no < 64; ++block_no) {
        if (LIKELY (chunk->c_active_mask & (1 << (block_no - 1)))) {
            ets_block_t *block = (ets_block_t *)((uint8_t *)chunk + block_no * ETS_BLOCK_SIZE);
            if (!has_lifted) {
                has_lifted = 1;
                (*lift) = block;
            } else {
                ets_heap_receive_applicant (root, block);
            }
        }
    }

    return has_lifted ? E_OK : E_FAIL;
}

static int ets_chunk_free (ets_chunk_t *chunk)
{
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
        ets_pages_free ((void *)locus, span * ETS_BLOCK_SIZE);
    }
    ets_chunk_tracker_t *const tracker = chunk->c_tracker;
    pthread_mutex_lock (&tracker->ct_access);
    if (chunk == __atomic_load_n (&tracker->ct_first, __ATOMIC_SEQ_CST)) {
        __atomic_store_n (&tracker->ct_first, chunk->c_next, __ATOMIC_SEQ_CST);
        if (chunk->c_next)
            chunk->c_next->c_prev = NULL;
    } else {
        chunk->c_prev->c_next = chunk->c_next;
        if (chunk->c_next)
            chunk->c_next->c_prev = chunk->c_prev;
    }
    pthread_mutex_unlock (&tracker->ct_access);

    /* free the header */
    const int r = ets_pages_free (chunk, ETS_BLOCK_SIZE);
    if (0 != r) return r;

    return E_OK;
}

static int ets_chunk_alloc (ets_chunk_t **chunkp)
{
    (*chunkp) = NULL;

    const int r = ets_pages_alloc_aligned ((void **)chunkp, ETS_CHUNK_SIZE, ETS_CHUNK_SIZE);
    if (0 != r)
        return r;
    (*chunkp)->c_next = NULL;
    (*chunkp)->c_tracker = NULL;
    (*chunkp)->c_active_mask = 0;
    (*chunkp)->c_nactive = 0;

    return E_OK;
}

/* SECTION: HEAP */

static int ets_heap_req_block_from_top (ets_heap_t *heap, size_t lkgi, ets_block_t **blockp)
{
    ets_chunk_t *chunk;
    {
        const int r = ets_chunk_alloc (&chunk);
        if (E_OK != r)
            return r;
    }

    ets_block_t *block;
    {
        const int r = ets_chunk_reserve_and_bind (chunk, &block, heap, &__ets_chunk_tracker);
        if (E_OK != r)
            return r;
    }
    {
        const int r = ets_block_format_to_size (block, ets_rlup_sli (lkgi));
        if (E_OK != r)
            return r;
    }
    (*blockp) = block;
    return E_OK;
}

static int ets_heap_req_block_from_heap (ets_heap_t *heap, size_t lkgi, ets_block_t **blockp)
{
    int r;
    r = ets_heap_req_block_from_slkg (&heap->h_lkgs[lkgi], blockp);
    if (r == E_OK) return E_OK;
    r = ets_heap_req_block_from_ulkg (&heap->h_lkgs[lkgi], ets_rlup_sli (lkgi), blockp);
    if (r == E_OK) return E_OK;

    if (!heap->h_owning_heap) {
        return ets_heap_req_block_from_top (heap, lkgi, blockp);
    } else {
        return ets_heap_req_block_from_heap (heap->h_owning_heap, lkgi, blockp);
    }
}

static int ets_heap_req_block_from_ulkg (ets_lkg_t *lkg, size_t osize, ets_block_t **blockp)
{
    size_t n_checked = 0;
    pthread_mutex_lock (&lkg->l_access);
    ets_block_t *block_cache = __atomic_load_n (&lkg->l_active, __ATOMIC_SEQ_CST);

#if 0
    ets_block_t *best_match{ NULL };
    size_t highest_priority{ 0 };
    while (block_cache != NULL) {
        const size_t priority = __atomic_load_n (&ets_get_chunk_for_block (block_cache)->c_priority);
        if (priority >= highest_priority) {
            best_match = block_cache;
            highest_priority = priority;
        }
        block_cache = block_cache->b_next;
    }
    if (!best_match) {
        pthread_mutex_unlock (&lkg->l_access);
        return E_FAIL;
    }
#endif
    pthread_mutex_lock (&block_cache->b_access);

    __atomic_store_n (&lkg->l_active, block_cache->b_next, __ATOMIC_SEQ_CST);
    if (block_cache->b_prev)
        block_cache->b_prev->b_next = block_cache->b_next;
    if (block_cache->b_next)
        block_cache->b_next->b_prev = block_cache->b_prev;

    pthread_mutex_unlock (&lkg->l_access);
    if (block_cache->b_osize != osize) {
        ets_block_format_to_size (block_cache, osize);
    }
    (*blockp) = block_cache;

    return E_OK;
}

static int ets_heap_req_block_from_slkg (ets_lkg_t *lkg, ets_block_t **blockp)
{
    pthread_mutex_lock (&lkg->l_access);
    ets_block_t *block_cache = __atomic_load_n (&lkg->l_active, __ATOMIC_SEQ_CST);

#if 0
    ets_block_t *best_match{ NULL };
    size_t highest_priority{ 0 };
    if (lkg->l_nblocks == 1) {
        pthread_mutex_lock (&block_cache->b_access);
        const size_t acnt_cache = __atomic_load_n (&block_cache->b_acnt, __ATOMIC_SEQ_CST);
        pthread_mutex_unlock (&block_cache->b_access);
        if (block_cache->b_ocnt == acnt_cache) {
            pthread_mutex_unlock (&lkg->l_access);
            return E_FAIL;
        }
    }
#endif
    _Bool found_match = 0;
    while (block_cache != NULL) {
        pthread_mutex_lock (&block_cache->b_access);
        if (NULL == __atomic_load_n (&block_cache->b_gfl, __ATOMIC_SEQ_CST)
            && NULL == __atomic_load_n (&block_cache->b_pfl, __ATOMIC_SEQ_CST)) {
            if (__atomic_load_n (&lkg->l_active, __ATOMIC_SEQ_CST) == block_cache) {
                if (block_cache->b_next != NULL || block_cache->b_prev == NULL)
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
            block_cache->b_next = NULL;
            block_cache->b_prev = NULL;
            pthread_mutex_unlock (&block_cache->b_access);
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
        pthread_mutex_unlock (&lkg->l_access);
        return E_FAIL;
    }
    pthread_mutex_lock (&block_cache->b_access);
    ets_block_t *curr_head = __atomic_load_n (&lkg->l_active, __ATOMIC_SEQ_CST);
    if (curr_head == block_cache) {
        /* if both b_next AND b_prev are NULL, it'll use b_next which is NULL */
        if (curr_head->b_next != NULL || curr_head->b_prev == NULL)
            __atomic_store_n (&lkg->l_active, curr_head->b_next, __ATOMIC_SEQ_CST);
        else
            __atomic_store_n (&lkg->l_active, curr_head->b_prev, __ATOMIC_SEQ_CST);
    }
    if (block_cache->b_prev)
        block_cache->b_prev->b_next = block_cache->b_next;
    if (block_cache->b_next)
        block_cache->b_next->b_prev = block_cache->b_prev;

    pthread_mutex_unlock (&lkg->l_access);
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
    return ets_heap_req_block_from_heap (heap->h_owning_heap, lkgi, blockp);
}

/* SECTION: BLOCK */

static int ets_block_init (ets_block_t *block)
{
    ets_opaque_block_t *opaque_block = (ets_opaque_block_t *)block;
    /* TODO: write these out properly */
    memset (block, 0, &opaque_block->b_memory[0] - (uint8_t *)block);

    pthread_mutex_init (&block->b_access, NULL);

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
    __atomic_store_n (&block->b_gfl, NULL, __ATOMIC_SEQ_CST);
    block->b_osize = osize;
    block->b_ocnt = (ETS_BLOCK_SIZE - sizeof (ets_block_t)) / osize;
    __atomic_store_n (&block->b_flags, 0, __ATOMIC_SEQ_CST);
    __atomic_store_n (&block->b_acnt, 0, __ATOMIC_SEQ_CST);

    memory += sizeof (ets_block_t);
    for (size_t i = 0; i < block->b_ocnt; ++i) {
        *(void **)memory = memory + osize;
        memory += osize;
    }

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
    if (LIKELY (block->b_pfl != NULL)) {
        return ets_block_alloc_object_impl (block, object);
    } else {
        pthread_mutex_lock (&block->b_access);
        /* not actually atomic, just needed an XCHG instruction */
        block->b_pfl = __atomic_exchange_n (&block->b_gfl, NULL, __ATOMIC_SEQ_CST);
        pthread_mutex_unlock (&block->b_access);

        if (LIKELY (block->b_pfl != NULL)) {
            return ets_block_alloc_object_impl (block, object);
        }
        return E_BL_EMPTY;
    }
}

static int ets_block_dealloc_object (ets_block_t *block, void *object)
{
    if (ets_tid () == __atomic_load_n (&block->b_owning_tid, __ATOMIC_SEQ_CST)) {
        *(void **)object = block->b_pfl;
        block->b_pfl = object;
    } else {
        pthread_mutex_lock (&block->b_access);
        *(void **)object = block->b_gfl;
        block->b_gfl = object;
        pthread_mutex_unlock (&block->b_access);
    }

    const size_t acnt_cache = __atomic_sub_fetch (&block->b_acnt, 1, __ATOMIC_SEQ_CST);
    if (0 == acnt_cache) {
        pthread_mutex_lock (&block->b_access);
        if (!(ETS_BLFL_HEAD & __atomic_load_n (&block->b_flags, __ATOMIC_SEQ_CST))) {
            if (0 == __atomic_load_n (&block->b_acnt, __ATOMIC_SEQ_CST)) {
                void *pfl_save = block->b_pfl,
                     *gfl_save = block->b_gfl;
                block->b_pfl = NULL;
                block->b_gfl = NULL;
                pthread_mutex_unlock (&block->b_access);

                ets_lkg_t *const lkg_cache = __atomic_load_n (&block->b_owning_lkg, __ATOMIC_SEQ_CST);
                pthread_mutex_lock (&lkg_cache->l_access);
                pthread_mutex_lock (&block->b_access);
                block->b_pfl = pfl_save;
                block->b_gfl = gfl_save;

                return ets_lkg_block_did_become_empty (lkg_cache, block);
            } else {
                pthread_mutex_unlock (&block->b_access);
                return E_OK;
            }
        } else {
            pthread_mutex_unlock (&block->b_access);
            return E_OK;
        }
    } else if (acnt_cache <= (block->b_ocnt / 2)) {
        pthread_mutex_lock (&block->b_access);
        /* concurrent accesses possible: allocation path */
        const uint16_t flag_cache = __atomic_load_n (&block->b_flags, __ATOMIC_SEQ_CST);
        if (!(ETS_BLFL_HEAD & flag_cache) && (ETS_BLFL_IN_THEATRE & flag_cache)) {
            if ((block->b_ocnt / 2) >= __atomic_load_n (&block->b_acnt, __ATOMIC_SEQ_CST)) {
                void *pfl_save = block->b_pfl;
                void *gfl_save = block->b_gfl;
                block->b_pfl = NULL;
                block->b_gfl = NULL;
                pthread_mutex_unlock (&block->b_access);

                /* issue: if a head transfer op is in progress
                 * possible situations:
                 *  - 0T a slide
                 *  - CANT be a pull
                 */
                ets_lkg_t *const lkg_cache = __atomic_load_n (&block->b_owning_lkg, __ATOMIC_SEQ_CST);
                pthread_mutex_lock (&lkg_cache->l_access);
                pthread_mutex_lock (&block->b_access);
                block->b_pfl = pfl_save;
                block->b_gfl = gfl_save;

                return ets_lkg_block_did_become_partially_empty (lkg_cache, block);
            } else {
                pthread_mutex_unlock (&block->b_access);
                return E_OK;
            }
        } else {
            pthread_mutex_unlock (&block->b_access);
            return E_OK;
        }
    }

    return E_OK;
}

/* SECTION: LINKAGE */

static int ets_lkg_alloc_object (ets_lkg_t *lkg, ets_heap_t *heap, void **object)
{
    ets_block_t *block_cache = __atomic_load_n (&lkg->l_active, __ATOMIC_SEQ_CST);
    if (UNLIKELY (block_cache == NULL)) {
        pthread_mutex_lock (&lkg->l_access);

        ets_block_t *tmp;
        int r = ets_lkg_req_block_from_heap (heap, lkg->l_index, &tmp);
        if (ETS_ISERR (r)) return r;
        __atomic_or_fetch (&tmp->b_flags, ETS_BLFL_HEAD, __ATOMIC_SEQ_CST);
        __atomic_store_n (&tmp->b_owning_tid, ets_tid (), __ATOMIC_SEQ_CST);

        __atomic_store_n (&tmp->b_owning_lkg, lkg, __ATOMIC_SEQ_CST);
        tmp->b_next = NULL;
        tmp->b_prev = NULL;
        __atomic_store_n (&lkg->l_active, tmp, __ATOMIC_SEQ_CST);

        pthread_mutex_unlock (&tmp->b_access);

        pthread_mutex_unlock (&lkg->l_access);
#if ETS_CHECK_PROMOTION_FAILURES
        int r = ets_block_alloc_object (tmp, object);
        if (r != E_OK) return E_LKG_SPOILED_PROMOTEE;
        return E_OK;
#else
        return ets_block_alloc_object (tmp, object);
#endif
    }

    if (LIKELY (E_OK == ets_block_alloc_object (block_cache, object))) {
        return E_OK;
    }

    pthread_mutex_lock (&lkg->l_access);
    pthread_mutex_lock (&block_cache->b_access);
    if (block_cache->b_next != NULL) {
        _Bool is_slideable = 1;
        for (;;) {
            pthread_mutex_lock (&block_cache->b_next->b_access);

            if (NULL == block_cache->b_next->b_gfl
                && NULL == block_cache->b_next->b_pfl
            /* double null free lists will ONLY occur naturally in
                     * head or left-of-head blocks */
#if 0
                    /* used to be acnt == 0 but we have to account for partials */
                    && block_cache->b_next->b_ocnt != __atomic_load_n(&block_cache->b_next->b_acnt, __ATOMIC_SEQ_CST)
#endif
            ) {
                ets_block_t *liftee = block_cache->b_next;
                block_cache->b_next = liftee->b_next;
                if (liftee->b_next != NULL)
                    liftee->b_next->b_prev = block_cache;
                /* cauterize - optional, but makes things easier*/
                liftee->b_next = NULL;
                liftee->b_prev = NULL;

                pthread_mutex_unlock (&liftee->b_access);
                if (block_cache->b_next == NULL) {
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
            __atomic_and_fetch (&block_cache->b_flags, ~ETS_BLFL_HEAD, __ATOMIC_SEQ_CST);
            __atomic_or_fetch (&block_cache->b_next->b_flags, ETS_BLFL_HEAD, __ATOMIC_SEQ_CST);

            block_cache->b_owning_lkg = lkg;

            __atomic_store_n (&lkg->l_active, block_cache->b_next, __ATOMIC_SEQ_CST);

            pthread_mutex_unlock (&block_cache->b_next->b_access);
            pthread_mutex_unlock (&block_cache->b_access);
            pthread_mutex_unlock (&lkg->l_access);

#if ETS_CHECK_PROMOTION_FAILURES
            int r = ets_block_alloc_object (block_cache->b_next, object);
            if (r != E_OK) return E_LKG_SPOILED_PROMOTEE;
            return E_OK;
#else
            return ets_block_alloc_object (block_cache->b_next, object);
#endif
        } else {
            /* 0 == is_slideable */
        }
    }

    ets_block_t *tmp;
    int r = ets_lkg_req_block_from_heap (heap, lkg->l_index, &tmp);
    if (ETS_ISERR (r)) return r;

    __atomic_or_fetch (&tmp->b_flags, ETS_BLFL_HEAD, __ATOMIC_SEQ_CST);
    __atomic_store_n (&tmp->b_owning_tid, ets_tid (), __ATOMIC_SEQ_CST);
    __atomic_store_n (&tmp->b_owning_lkg, lkg, __ATOMIC_SEQ_CST);

    __atomic_and_fetch (&block_cache->b_flags, ~ETS_BLFL_HEAD, __ATOMIC_SEQ_CST);
    tmp->b_prev = block_cache;
    tmp->b_next = block_cache->b_next;
    if (tmp->b_next != NULL)
        tmp->b_next->b_prev = tmp;
    __atomic_store_n (&lkg->l_active, tmp, __ATOMIC_SEQ_CST);
    pthread_mutex_unlock (&tmp->b_access);

    pthread_mutex_unlock (&block_cache->b_access);
    pthread_mutex_unlock (&lkg->l_access);

#if ETS_CHECK_PROMOTION_FAILURES
    int r = ets_block_alloc_object (tmp, object);
    if (r != E_OK) return E_LKG_SPOILED_PROMOTEE;
    return E_OK;
#else
    return ets_block_alloc_object (tmp, object);
#endif
}
