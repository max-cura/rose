#include <iostream>
#include <assert.h>

#if !defined(NALLOC)
    #define NALLOC 0x1000000L
#endif

#if DO_BENCHMARK
    #include <benchmark/benchmark.h>

static void BM_ETSRunthrough (benchmark::State &state)
{
    void **objects = (void **)malloc (sizeof *objects * NALLOC);
    int size = 0x80;

    int r;
    pthread_mutex_init (&__ets_chunk_tracker.ct_access, NULL);
    //ets_chunk_t *chunk = NULL;
    //r = ets_chunk_alloc (&chunk);
    //printf ("[test]\tallocated chunk at %p (r = %i)\n", chunk, r);
    if (ETS_ISERR (r)) return;
    ets_heap_t *tl_heap = (ets_heap_t *)malloc (24 + 20 * sizeof (ets_lkg_t));
    tl_heap->h_owning_heap = NULL;
    tl_heap->h_nlkgs = 20;
    for (size_t i = 0; i < tl_heap->h_nlkgs; ++i) {
        ets_lkg_init (&tl_heap->h_lkgs[i], i, tl_heap);
    }
    //r = ets_chunk_bind (chunk, tl_heap, &__ets_chunk_tracker);
    //printf ("[test]\tets_chunk_bind (r = %i)\n", r);

    srand (0);

    for (auto _ : state) {
        for (int i = 0; i < NALLOC; i++) {
            /*r = */ ets_heap_alloc_object (tl_heap, (void **)&objects[i], 1 + (rand () % 511));
            //printf ("[test]\tets_heap_alloc_object[size=%i] (r = %i, object=%p)\n", size, r, objects[i]);
        }
        for (int i = 0; i < NALLOC; i++) {
            /*r = */ ets_dealloc_object ((void *)objects[i]);
            //printf ("[test]\tets_dealloc_object[object=%p] (r = %i)\n", objects[i], r);
        }
    }
    free (objects);
}

static void BM_MallocRunthrough (benchmark::State &state)
{
    void **objects = (void **)malloc (sizeof *objects * NALLOC);
    int size = 0x80;

    srand (0);

    for (auto _ : state) {
        for (int i = 0; i < NALLOC; i++) {
            objects[i] = malloc (rand () % 512);
        }
        for (int i = 0; i < NALLOC; i++) {
            free ((void *)objects[i]);
        }
    }
    free (objects);
}

BENCHMARK (BM_ETSRunthrough);
BENCHMARK (BM_MallocRunthrough);
BENCHMARK_MAIN ();
#else

int main (int argc, char **argv)
{
    void **objects = (void **)malloc (sizeof *objects * NALLOC);
    int size = 0x80;

    int r;
    pthread_mutex_init (&__ets_chunk_tracker.ct_access, NULL);
    ets_chunk_t *chunk = NULL;
    r = ets_chunk_alloc (&chunk);
    //printf ("[test]\tallocated chunk at %p (r = %i)\n", chunk, r);
    if (ETS_ISERR (r)) return 1;
    ets_heap_t *tl_heap = (ets_heap_t *)malloc (24 + 20 * sizeof (ets_lkg_t));
    tl_heap->h_owning_heap = NULL;
    tl_heap->h_nlkgs = 20;
    for (size_t i = 0; i < tl_heap->h_nlkgs; ++i) {
        ets_lkg_init (&tl_heap->h_lkgs[i], i, tl_heap);
    }
    r = ets_chunk_bind (chunk, tl_heap, &__ets_chunk_tracker);
    //printf ("[test]\tets_chunk_bind (r = %i)\n", r);

    srand (0);

    for (int i = 0; i < NALLOC; i++) {
        /*r = */ ets_heap_alloc_object (tl_heap, (void **)&objects[i], 1 + (rand () % 511));
        //printf ("[test]\tets_heap_alloc_object[size=%i] (r = %i, object=%p)\n", size, r, objects[i]);
    }
    for (int i = 0; i < NALLOC; i++) {
        /*r = */ ets_dealloc_object ((void *)objects[i]);
        //printf ("[test]\tets_dealloc_object[object=%p] (r = %i)\n", objects[i], r);
    }
    free (objects);

    return 0;
}

#endif
