/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef THREADLOCALPOOL_H_
#define THREADLOCALPOOL_H_

#include "boost/pool/pool.hpp"
#include "boost/shared_ptr.hpp"

#include "structures/CompactingSet.h"

namespace voltdb {

/**
 * A wrapper around a set of pools that are local to the current thread.
 * An instance of the thread local pool must be maintained somewhere in the thread to ensure initialization
 * and destruction of the thread local pools. Creating multiple instances is fine, it is reference counted. The thread local
 * instance of pools will be freed once the last ThreadLocalPool reference in the thread is destructed.
 */
class ThreadLocalPool {
public:
    ThreadLocalPool();
    ~ThreadLocalPool();

    /// The layout of an allocation segregated by size,
    /// including overhead to help identify the size-specific
    /// pool from which the allocation must be freed.
    /// Uses placement new and a constructor to overlay onto
    /// the variable-length raw internal allocation and
    /// initialize the requested size as a prefix field.
    /// The m_data field makes it easy to access the user
    /// data at its fixed offset.
    struct Sized {
        int32_t m_size;
        char m_data[0];
        Sized(int32_t requested_size) : m_size(requested_size) { }
    };

    static const int POOLED_MAX_VALUE_LENGTH;

    /**
     * Allocate space from a page of objects of the requested size.
     * Each new size of object splinters the allocated memory into a new pool
     * which is a collection of pages of objects of that exact size.
     * Each pool will allocate additional space that is initally unused.
     * This is not an issue when the allocated objects will be instances of a
     * class that has many instances to quickly fill up the unused space. So,
     * an optimal use case is a custom operator new for a commonly used class.
     * Page sizes in a pool may vary as the number of required pages grows,
     * but will be bounded to 2MB or to the size of two objects if they are
     * larger than 256KB (not typical). There is no fixed upper limit to the
     * size of object that can be requested.
     * This allocation method would be a poor choice for variable-length
     * buffers whose sizes depend on user input and may be unlikely to repeat.
     * allocateRelocatable is the better fit for that use case.
     */
    static void* allocateExactSizedObject(std::size_t size);

    /**
     * Deallocate the object returned by allocateExactSizedObject.
     */
    static void freeExactSizedObject(std::size_t, void* object);

    static std::size_t getPoolAllocationSize();

    /**
     * Allocate space from a page of objects of approximately the requested
     * size. There will be relatively small gaps of unused space between the
     * objects. This is caused by aligning them to a slightly larger size.
     * This allows allocations within a pool of similarly-sized objects
     * to always fit when they are relocated to fill a hole left by a
     * deallocation. This enables continuous compaction to prevent deallocation
     * from accumulating large unused holes in the page.
     * For the relocation to work, there can only be one persistent pointer
     * to an allocation and the pointer's address must be registered with the
     * allocator so that the allocator can reset the pointer at that address
     * when its referent needs to be relocated.
     * Allocation requests of greater than 1 megabyte + 12 bytes will throw a
     * fatal exception. This limit is arbitrary and could be extended if
     * needed. The caller is expected to guard against this fatal condition.
     * This allocation method is ideal for variable-length user data that is
     * managed through a single point of reference (See class StringRef).
     * The relocation feature makes this allocation method a poor choice for
     * objects that could be referenced by multiple persistent pointers.
     * allocateExactSizedObject uses a simpler, more general allocator that
     * works well with fixed-sized allocations and counted references.
     * Also, the sole persistent pointer is assumed to remain at a fixed
     * address for the lifetime of the allocation, but it would be easy to add
     * a function that allowed the persistent pointer to be safely relocated
     * and re-registered.
     */
    static Sized* allocateRelocatable(char** referrer, int32_t sz);

    /**
     * Return the rounded-up buffer size that was allocated for the string.
     */
    static int32_t getAllocationSizeForRelocatable(Sized* string);

    /**
     * Deallocate the object returned by allocateRelocatable.
     * This implements continuous compaction which can have the side effect of
     * relocating some other allocation.
     *
     * Note that in deferred release mode (described below in the
     * header comment for ScopedPoolDeferredReleaseMode), we may not
     * immediately free the object, but mark it for deletion at a
     * later point in time.
     */
    static void freeRelocatable(Sized* string);
};

/**
 * A helper class to switch to deferred release mode within the
 * current scope, switching back to immediate release mode in an
 * exception-safe way.
 *
 * Deferred release mode means that objects allocated via
 * ThreadLocalPool::allocateRelocatable won't be freed immediately
 * when freeRelocatable is called.  Instead, they will be freed when
 * this object goes out of scope.
 *
 * This is desirable because the hole-filling algorithm CompactingPool
 * uses to keep memory contiguous can incur needless memcpying.
 * Freeing many objects at once can allows us to avoid copying objects
 * that are soon to be deleted anyway.
 */
struct ScopedPoolDeferredReleaseMode {
    ScopedPoolDeferredReleaseMode();
    ~ScopedPoolDeferredReleaseMode();
};


typedef std::pair<int32_t, void*> SizePtrPair;

struct SizePtrPairComparator {
    int operator() (const SizePtrPair& v1, const SizePtrPair& v2) const {

        if (v1.first < v2.first) {
            return -1;
        }

        if (v1.first > v2.first) {
            return 1;
        }

        const char* cv1 = static_cast<const char*>(v1.second);
        const char* cv2 = static_cast<const char*>(v2.second);
        if (cv1 < cv2) {
            return -1;
        }

        if (cv1 > cv2) {
            return 1;
        }

        return 0;
    }
};

typedef CompactingSet<SizePtrPair, SizePtrPairComparator> SizePtrPairSet;

}



#endif /* THREADLOCALPOOL_H_ */
