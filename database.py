import os
import asyncio
import logging
import libsql_client

# Environment variables pulled automatically from Railway
TURSO_URL = os.environ.get("TURSO_DATABASE_URL")
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")

# Lock to guarantee atomic write operations
_db_write_lock = asyncio.Lock()


def _safe_params(params):
    """Converts 64-bit Discord integer IDs to strings to prevent 

    truncation or precision loss over Turso's Hrana HTTP connection[span_1](start_span)[span_1](end_span).
    """
    if params is None:
        return ()
    if isinstance(params, (list, tuple)):
        cleaned = []
        for p in params:
            if isinstance(p, int) and (p > 9007199254740991 or p < -9007199254740991):
                cleaned.append(str(p))
            else:
                cleaned.append(p)
        return tuple(cleaned)
    if isinstance(params, dict):
        cleaned = {}
        for k, v in params.items():
            if isinstance(v, int) and (v > 9007199254740991 or v < -9007199254740991):
                cleaned[k] = str(v)
            else:
                cleaned[k] = v
        return cleaned
    return params


class ResilientCursor:
    """Handles connection persistence and automatic retries for Turso[span_2](start_span)[span_2](end_span)."""

    def __init__(self):
        self.client = None

    async def _get_client(self):
        if self.client is None:
            self.client = libsql_client.create_client_async(
                url=TURSO_URL,
                auth_token=TURSO_TOKEN
            )
        return self.client

    async def aexecute(self, query: str, params=None, commit: bool = False):
        client = await self._get_client()
        safe_p = _safe_params(params)
        max_retries = 3

        for attempt in range(max_retries):
            try:
                result = await client.execute(query, safe_p)
                return result
            except Exception as e:
                logging.warning(f"Turso query attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise e
                self.client = None
                await asyncio.sleep(1)

    async def close(self):
        if self.client:
            await self.client.close()
            self.client = None


# Global cursor instance
cursor = ResilientCursor()


async def adb_write_commit(func):
    """Executes write operations inside an async lock to prevent race conditions[span_3](start_span)[span_3](end_span)."""
    async with _db_write_lock:
        return await func()


async def init_db():
    """Initializes basic database tables on bot startup."""
    queries = [
        """
        CREATE TABLE IF NOT EXISTS tempbans (
            user_id TEXT,
            guild_id TEXT,
            unban_time REAL,
            PRIMARY KEY (user_id, guild_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS xp (
            user_id TEXT,
            guild_id TEXT,
            xp_amount INTEGER DEFAULT 0,
            level INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, guild_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS warns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            guild_id TEXT,
            reason TEXT,
            timestamp REAL
        );
        """
    ]

    async def _setup():
        for q in queries:
            await cursor.aexecute(q)

    await adb_write_commit(_setup)
