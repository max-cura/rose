/* AUTHOR Maximilien M. Cura
 */

#pragma once

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>

struct ets_lkg;

#define PRECONDITION(s)

#define ETS_BLOCK_SIZE 0x4000L

//! Block of memory in a chunk.
typedef struct ets_block
{
    void *b_pfl, *b_gfl;

    uint8_t b_flags;
    uint8_t b_flisroh;
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

    uint8_t b_flags;
    uint8_t b_flisroh;
    uint16_t b_ocnt;
    uint16_t b_acnt;
    uint16_t b_osize;

    struct ets_block *b_prev, *b_next;
    struct ets_lkg *b_owning_lkg;
    uint64_t b_owning_tid;

    pthread_mutex_t b_gaccess;
    uint8_t b_memory[ETS_BLOCK_SIZE - sizeof (ets_block_t)];
} ets_opaque_block_t;
//! Get the block that owns the object at the specified memory location.
//! Thread-safe:1
inline ets_block_t *ets_get_block_for_object (void *object)
{
    return (ets_block_t *)(void *)((uintptr_t)object & ~(ETS_BLOCK_SIZE - 1));
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

//! Either local or regional; global is hardcoded as a NULL value in
//! `h_owning_heap`.
//! 0TE: IF ADDING OR SUBTRACTING MEMBERS, REMEMBER TO MODIFY ets_get_heap_for_lkg
typedef struct ets_heap
{
    size_t h_owned_heaps;
    struct ets_heap *h_owning_heap;
    size_t h_nlkgs;
    ets_lkg_t h_lkgs[];
} ets_heap_t;

struct ets_arena;

static inline ets_heap_t *ets_get_heap_for_lkg (ets_lkg_t *lkg)
{
    return (ets_heap_t *)((uint8_t *)(lkg - lkg->l_index) - sizeof (ets_heap_t *) - sizeof (size_t) - sizeof (size_t));
}

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

inline ets_chunk_t *ets_get_chunk_for_block (ets_block_t *block)
{
    return (ets_chunk_t *)(void *)((uintptr_t)block & ~(ETS_CHUNK_SIZE - 1));
}
inline size_t ets_get_block_no (ets_block_t *block)
{
    return (((uintptr_t)block & ~(ETS_CHUNK_SIZE - 1)) / ETS_BLOCK_SIZE) - 1;
}
