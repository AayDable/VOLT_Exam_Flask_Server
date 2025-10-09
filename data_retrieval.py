# pgsql_client.py (CLEANED VERSION)
"""
PostgreSQL Async Client
=======================

Handles database connections and query execution.
Separated from caching concerns for better modularity.
"""

import pandas as pd

class PGSQLData:
    def __init__(self):
        self.pool = None

    async def execute_query(self, query):
        """
        Execute a query and return results as list of dicts.
        
        Args:
            query: SQL query string
            
        Returns:
            List of dictionaries representing rows
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            data = [dict(r) for r in rows]
        return pd.DataFrame(data)

if __name__ == '__main__':
    print('Done!')
