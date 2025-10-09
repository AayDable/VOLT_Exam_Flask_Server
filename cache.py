# cache.py
"""
Caching Layer for Database Queries
===================================

Provides thread-safe caching with expiration for expensive database operations.
Uses double-checked locking pattern to prevent cache stampede.
Uses function name as cache key for automatic key management.
"""

import asyncio
from expiringdict import ExpiringDict
from copy import deepcopy

class CacheManager:
    def __init__(self, max_len=100, max_age_seconds=20):
        self.cache = ExpiringDict(max_len=max_len, max_age_seconds=max_age_seconds)
        self.cache_locks = {}  # Stores per-key locks
        self.global_lock = asyncio.Lock()  # Protects lock creation

    async def get_or_fetch(self, fetch_func):
        """
        Retrieve data from cache or fetch using provided async function.
        Uses function name as the cache key.
        
        Args:
            fetch_func: Async callable that fetches data if cache miss.
            
        Returns:
            Cached or freshly fetched data
        """
        # Use function name as cache key
        key = fetch_func.__name__
        
        # ✅ FIRST CHECK (No Lock) - Fast path for cache hits
        if key in self.cache:
            # print(f"✅ Cache hit for {key} - returning cached data")
            return deepcopy(self.cache[key])
        
        # ⚠️ GLOBAL LOCK - Protects per-key lock creation
        async with self.global_lock:
            # print(f"🔒 Acquired global lock for {key}")
            
            # Get or create the per-key lock safely
            if key not in self.cache_locks:
                self.cache_locks[key] = asyncio.Lock()
                # print(f"🔑 Created new lock for key: {key}")
            # else:
                # print(f"🔑 Reusing existing lock for key: {key}")

            lock = self.cache_locks[key]
            # print(f"🔓 Released global lock for {key}")
        
        # ⚠️ PER-KEY LOCK - Only ONE coroutine per key can enter
        async with lock:
            # print(f"🔒 Acquired per-key lock for {key}")
            
            # ✅ DOUBLE-CHECK - Second cache check after acquiring lock
            if key in self.cache:
                # print(f"✅ Cache hit after lock (another coroutine cached it)")
                return deepcopy(self.cache[key])
            
            # 🔥 FETCH DATA - Only the FIRST coroutine reaches here
            data = await fetch_func()
            # print(f'🔥 DATA FETCHED for {key}')
            
            # 💾 CACHE IT
            self.cache[key] = data
            # print(f"💾 Cached data for {key}")
            return deepcopy(self.cache[key])

    def invalidate(self, func_or_name):
        """
        Remove specific key from cache.
        
        Args:
            func_or_name: Function object or string name of function to invalidate
        """
        key = func_or_name.__name__ if callable(func_or_name) else func_or_name
        
        if key in self.cache:
            del self.cache[key]
            # print(f"🗑️ Invalidated cache for {key}")

    def clear(self):
        """Clear entire cache."""
        self.cache.clear()
        self.cache_locks.clear()
        # print("🗑️ Cache cleared")


# Global cache instance
cache_manager = CacheManager(max_len=100, max_age_seconds=20)

if __name__ == '__main__':
    from data_preprocessing.first_layer_fns import l1_get_proper_dashboard_data_unprocessed,l1_get_rawdata_cleaned
    async def single_task():
        data = await cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed)
        return data
    
    async def multiple_tasks():
        d1,d2,d3,d4,d5 = await asyncio.gather(
            cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed),
            cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed),
            cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed),
            cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed),
            cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed)
        )

        return d1,d2,d3,d4,d5
    
    async def multiple_different_tasks():
        d1,d2,d3,d4,d5 = await asyncio.gather(
            cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed),
            cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed),
            cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed),
            cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed),
            cache_manager.get_or_fetch(l1_get_rawdata_cleaned)
        )

        return d1,d2,d3,d4,d5
    
    # data = asyncio.run(single_task())

    d1,d2,d3,d4,d5 = asyncio.run(multiple_different_tasks())
    
    print('hi')