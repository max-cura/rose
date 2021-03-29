/*
 * PROJECT
 * AUTHOR Maximilien M. Cura
 *
 * FILE archive.h
 * DESC
 */

/*
 * COPYRIGHT (c) 2021 Maximilien M. Cura. ALL RIGHTS RESERVED.
 * USE BY EXPLICIT WRITTEN PERMISSION ONLY.
 */

#ifndef __ROSE_ARCHIVE
#define __ROSE_ARCHIVE

#include <stdint.h>
//#include <stddef.h>
#include <pthread.h>

#include <vector>
#include <type_traits>
#include <algorithm>

#include <rose/rt/maybe.h>

namespace rose::archive {
    typedef uint64_t Id;
    typedef Id TypeId;

    template <typename...>
    struct IdGenerator
    {
        inline static Id __counter{ 0 };
        constexpr const inline static Id null_id{ 0 };

        [[nodiscard]] inline static const Id next ()
        {
            return __atomic_add_fetch (&__counter, 1, __ATOMIC_RELAXED);
        }
    };

    enum tag_component {};

    template <typename M = tag_component>
    struct TypeIdGenerator
    {
        constexpr const inline static TypeId null_type = IdGenerator<M>::null_id;

        template <typename>
        static inline TypeId __id{ 0 };
        template <typename>
        static inline pthread_once_t __id_once = PTHREAD_ONCE_INIT;

        template <typename T>
        static void __id_init ()
        {
            __atomic_store_n (&__id<T>, IdGenerator<M>::next (), __ATOMIC_RELEASE);
        }

        template <typename T>
        [[nodiscard]] inline static TypeId generate ()
        {
            TypeId read_id = __atomic_load_n (&__id<T>, __ATOMIC_ACQUIRE);
            if (__builtin_expect (read_id == 0, 0)) {
                return read_id;
            } else {
                pthread_once (&__id_once<T>, __id_init<T>);
                return __atomic_load_n (&__id<T>, __ATOMIC_ACQUIRE);
            }
        }
    };

    template <typename T, typename M = tag_component>
    constexpr inline const TypeId type_id = TypeIdGenerator<M>::template generate<T> ();
    template <typename M = tag_component>
    constexpr inline const TypeId null_id = TypeIdGenerator<M>::null_type;

    template <size_t>
    struct IdSet;
    struct MutIdSet;

    template <typename>
    struct _IsFixedIdSet : std::integral_constant<bool, 0>
    { };
    template <size_t N>
    struct _IsFixedIdSet<IdSet<N>> : std::integral_constant<bool, 1>
    { };
    template <typename T>
    constexpr inline bool is_id_set_noref = std::is_same_v<T, MutIdSet> || _IsFixedIdSet<T>::value;
    template <typename T>
    constexpr inline bool is_id_set = is_id_set_noref<std::remove_reference_t<T>>;

    template <size_t N>
    struct IdSet
    {
        Id inner[N];

        inline constexpr explicit IdSet (const Id ids[N])
        {
            for (size_t i = 0; i < N; ++i)
                inner[i] = ids[i];
            std::sort (inner, inner + N);
        }

        inline constexpr explicit IdSet (const std::initializer_list<Id> ids)
        {
            auto iter = ids.begin ();
            for (size_t i = 0; i < N; ++i)
                inner[i] = *iter++;
            std::sort (inner, inner + N);
        }

        [[nodiscard]] constexpr inline size_t size () const
        {
            return N;
        }

        [[nodiscard]] constexpr bool contains (Id id) const
        {
            if (id > inner[N - 1])
                return false;
            for (size_t i = 0; i < N; ++i) {
                if (id == inner[i])
                    return true;
                if (id < inner[i])
                    return false;
            }
            __builtin_unreachable ();
        }

        template <typename T>
        requires is_id_set<T> [[nodiscard]] constexpr bool operator== (T rhs)
        {
            if (N != rhs.size ())
                return false;
            for (size_t i = 0; i < N; ++i)
                if (inner[i] != rhs.inner[i])
                    return false;
            return true;
        }

        [[nodiscard]] Id const &operator[] (size_t i) const
        {
            return inner[i];
        }
        [[nodiscard]] Id &operator[] (size_t i)
        {
            return inner[i];
        }
    };

    struct MutIdSet
    {
        std::vector<Id> inner;

        inline MutIdSet () = default;
        template <size_t N>
        inline explicit MutIdSet (const IdSet<N> &id_set)
            : inner{ id_set.inner, id_set.inner + N }
        { }
        template <size_t N>
        inline explicit MutIdSet (IdSet<N> &&id_set)
            : inner{ id_set.inner, id_set.inner + N }
        { }
        inline MutIdSet (MutIdSet const &id_set) = default;

        MutIdSet &operator= (MutIdSet const &id_set) = default;
        MutIdSet &operator= (MutIdSet &&id_set) = default;
        template <size_t N>
        MutIdSet &operator= (IdSet<N> const &id_set)
        {
            inner = std::move (std::vector{ id_set.inner, id_set.inner + N });
            return *this;
        }

        [[nodiscard]] inline size_t size () const
        {
            return inner.size ();
        }

        [[nodiscard]] constexpr bool contains (Id id) const
        {
            const size_t N = size ();
            if (id > inner[N - 1])
                return false;
            for (size_t i = 0; i < N; ++i) {
                if (id == inner[i])
                    return true;
                if (id < inner[i])
                    return false;
            }
            __builtin_unreachable ();
        }

        template <typename T>
        requires is_id_set<T> [[nodiscard]] constexpr bool operator== (T rhs)
        {
            const size_t N = size ();
            if (N != rhs.size ())
                return false;
            for (size_t i = 0; i < N; ++i)
                if (inner[i] != rhs.inner[i])
                    return false;
            return true;
        }

        [[nodiscard]] Id const &operator[] (size_t i) const
        {
            return inner[i];
        }
        [[nodiscard]] Id &operator[] (size_t i)
        {
            return inner[i];
        }
    };

    template <typename... TT>
    [[nodiscard]] constexpr IdSet<sizeof...(TT)> type_ids ()
    {
        return IdSet<sizeof...(TT)>{ type_id<TT, tag_component>... };
    }

    struct TypeInfo
    {
        TypeId id;
        /* reserved */
    };

    struct TypeRegistry
    {
        /* aight so we got some problems
         *  1) TypeRegistry needs to be concurrent
         *  2) Assumption: entries will not be changed (through the registry)
         *      after they are created.
         *
         * Invariant: once register_type(id) has been called, info(id) will always
         *            be valid.
         */

        std::shared_ptr<TypeInfo **> lookup_table;
        size_t n_types;
        pthread_rwlock_t table_lock;

        TypeRegistry ()
        {
            n_types = 1;
            lookup_table = std::make_shared<TypeInfo **> ((TypeInfo **)new TypeInfo *[n_types]);

            (*lookup_table.get ())[0] = new TypeInfo{ null_id<tag_component> };
        }

        /* Assumption: no concurrent accesses */
        ~TypeRegistry ()
        {
            for (size_t i = 0; i < n_types; ++i) {
                if (lookup_table[i])
                    delete lookup_table[i];
            }
        }

        [[nodiscard]] TypeInfo const *const info (TypeId id) const
        {
            if (id >= __atomic_load_n (&n_types, __ATOMIC_SEQ_CST)) {
                return nullptr;
            }
            TypeInfo *ret;
            {
                std::shared_ptr<TypeInfo **> table{ lookup_table };
                ret = (*table.get ())[id];
            }
            return ret;
        }

        [[nodiscard]] TypeInfo *info (TypeId id)
        {
            if (id >= __atomic_load_n (&n_types, __ATOMIC_SEQ_CST)) {
                return nullptr;
            }
            TypeInfo *ret;
            {
                std::shared_ptr<TypeInfo **> table{ lookup_table };
                ret = (*table.get ())[id];
            }
            return ret;
        }

        [[nodiscard]] bool valid (TypeId id) const
        {
            if (id >= __atomic_load_n (&n_types, __ATOMIC_SEQ_CST)) {
                return false;
            }
            std::shared_ptr<TypeInfo **> table{ lookup_table };
            return (*table.get ())[id] != nullptr;
        }
        template <typename T>
        [[nodiscard]] bool valid () const
        {
            return valid (type_id<T, tag_component>);
        }

        void register_type (TypeId id)
        {
            if (id >= __atomic_load_n (&n_types, __ATOMIC_SEQ_CST)) {
                pthread_rwlock_wrlock (&table_lock);
                if (id >= __atomic_load_n (&n_types, __ATOMIC_SEQ_CST)) {
                    std::shared_ptr<TypeInfo **> old_table{ lookup_table };
                    TypeInfo **_old_table = *old_table.get ();
                    TypeInfo **new_table = new TypeInfo *[id + 1] { nullptr };
                    memcpy (new_table, _old_table, __atomic_load_n (&n_types, __ATOMIC_SEQ_CST) * sizeof *_old_table);
                    lookup_table = std::make_shared<TypeInfo **> (new_table);
                    __atomic_store_n (&n_types, id + 1, __ATOMIC_SEQ_CST);
                }
                pthread_rwlock_unlock (&table_lock);
            }
            {
                std::shared_ptr<TypeInfo **> table{ lookup_table };
                if ((*table.get ())[id] == nullptr) {
                    pthread_rwlock_rdlock (&table_lock);
                    (*table.get ())[id] = new TypeInfo{ id };
                    pthread_rwlock_unlock (&table_lock);
                }
            }
        }

        inline void try_register_type (TypeId id)
        {
            if (!valid (id))
                register_type (id);
        }

        template <typename... TT>
        void assure ()
        {
            auto id_set = type_ids<TT...> ();
            for (size_t i = 0; i < sizeof...(TT); ++i)
                try_register_type (id_set[i]);
        }
    };

    typedef uint64_t ArchetypeId;
    typedef uint64_t AssociationIndex;

    namespace detail {
        template <typename T>
        requires std::is_integral_v<T> consteval T __ce_max (T a, T b)
        {
            return a < b ? b : a;
        }

        template <typename H, typename... TT>
        struct _FirstType
        {
            typedef H type;
        };

        template <typename... TT>
        using first_type_t = typename _FirstType<TT...>::type;
    }

    struct ArchetypeStorage
    {
        virtual ~ArchetypeStorage () = default;
        [[nodiscard]] virtual size_t id_to_index (TypeId) const = 0;
        [[nodiscard]] virtual TypeId index_to_id (size_t) const = 0;
        [[nodiscard]] virtual size_t id_to_offset (TypeId) const = 0;
    };

    template <typename... TT>
    struct _ArchetypeStorage : public ArchetypeStorage
    {
        static inline constinit const size_t width = (sizeof (TT) + ... + 0);
        //        static inline constinit const size_t entry_align = alignof (detail::first_type_t<TT...>);
        static inline size_t sizes[sizeof...(TT)];
        static inline size_t offsets[sizeof...(TT)];
        static inline IdSet<sizeof...(TT)> id_set{ type_ids<TT...> () };
        static inline size_t convert_id[id_set[sizeof...(TT) - 1]];
        static inline size_t convert_index[sizeof...(TT)];

        static inline void __init_family ()
        {
            size_t unordered_sizes[]{ sizeof (TT)... };
            TypeId unordered_ids[]{ type_id<TT, tag_component>... };
            size_t packed_accum = 0;
            memset (&convert_id, 0, sizeof convert_id);
            memset (&convert_id, 0, sizeof convert_index);
            for (size_t i = 0; i < sizeof...(TT); ++i)
                for (size_t j = 0; j < sizeof...(TT); ++j)
                    if (unordered_ids[j] == id_set.inner[i]) {
                        offsets[i] = packed_accum;
                        sizes[i] = unordered_sizes[j];
                        convert_id[id_set.inner[i]] = j;
                        convert_index[j] = i;
                        packed_accum += sizes[i];
                    }
        }

        [[nodiscard]] static size_t __id_to_index (TypeId id)
        {
            return convert_id[id];
        }
        [[nodiscard]] static TypeId __index_to_id (size_t index)
        {
            return convert_index[index];
        }
        [[nodiscard]] static size_t __id_to_offset (TypeId id)
        {
            return offsets[__id_to_index (id)];
        }

        template <typename U>
        [[nodiscard]] static inline size_t index ()
        {
            const static size_t __cached_index = __id_to_index (type_id<U, tag_component>);
            return __cached_index;
        }

        template <typename U>
        [[nodiscard]] static inline size_t offset ()
        {
            const static size_t __cached_offset = __id_to_offset (type_id<U, tag_component>);
            return __cached_offset;
        }

        /* SECTION: INSTANCE MEMBERS */


        inline static pthread_once_t __family_init = PTHREAD_ONCE_INIT;

        struct RowType
        {
            uint8_t data[detail::__ce_max<size_t> (width, sizeof (AssociationIndex))];
        };

        std::vector<RowType> rows{};
        static constexpr inline const AssociationIndex freelist_terminator = 0;
        AssociationIndex freelist_head{ freelist_terminator };

        _ArchetypeStorage ()
            : ArchetypeStorage{}
        {
            pthread_once (&__family_init, _ArchetypeStorage<TT...>::__init_family);
            rows.push_back (RowType{});
        }
        ~_ArchetypeStorage () override = default;

        [[nodiscard]] size_t id_to_index (TypeId id) const override
        {
            return __id_to_index (id);
        }
        [[nodiscard]] TypeId index_to_id (size_t index) const override
        {
            return __index_to_id (index);
        }
        [[nodiscard]] size_t id_to_offset (TypeId id) const override
        {
            return __id_to_offset (id);
        }
    };
}

#endif /* !@__ROSE_ARCHIVE */
