import asyncio
import json
import logging
from typing import Any, Dict, Mapping, Optional

from aiogram.exceptions import DataNotDictLikeError
from aiogram.fsm.state import State
from aiogram.fsm.storage.base import BaseStorage, StorageKey, StateType
from psycopg2.extras import Json


def decode_fsm_data(value):
    if value is None or value == "":
        return {}
    if isinstance(value, dict):
        return value.copy()
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            logging.warning("POSTGRES_FSM_DATA_INVALID_JSON")
            return {}
        if isinstance(decoded, dict):
            return decoded.copy()
        logging.warning("POSTGRES_FSM_DATA_NON_OBJECT: value_type=%s", type(decoded).__name__)
        return {}
    logging.warning("POSTGRES_FSM_DATA_UNEXPECTED_TYPE: value_type=%s", type(value).__name__)
    return {}


def cleanup_postgres_fsm_storage(conn_factory, older_than_days=30):
    conn = conn_factory()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE FROM aiogram_fsm_states
            WHERE updated_at < NOW() - (%s * INTERVAL '1 day')
            """,
            (int(older_than_days),),
        )
        deleted = getattr(cur, "rowcount", None)
        conn.commit()
        logging.info("POSTGRES_FSM_STORAGE_CLEANUP: deleted=%s", deleted if deleted is not None else "unknown")
        return deleted if deleted is not None else 0
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


class PostgresFSMStorage(BaseStorage):
    def __init__(self, conn_factory):
        self.conn_factory = conn_factory

    def key_params(self, key: StorageKey):
        return (
            int(key.bot_id),
            int(key.chat_id),
            int(key.user_id),
            int(key.thread_id or 0),
            key.business_connection_id or "",
            key.destiny,
        )

    async def set_state(self, key: StorageKey, state: StateType = None) -> None:
        state_value = state.state if isinstance(state, State) else state
        await asyncio.to_thread(self._set_state_sync, key, state_value)

    async def get_state(self, key: StorageKey) -> Optional[str]:
        return await asyncio.to_thread(self._get_state_sync, key)

    async def set_data(self, key: StorageKey, data: Mapping[str, Any]) -> None:
        if not isinstance(data, dict):
            raise DataNotDictLikeError(f"Data must be a dict or dict-like object, got {type(data).__name__}")
        await asyncio.to_thread(self._set_data_sync, key, data.copy())

    async def get_data(self, key: StorageKey) -> Dict[str, Any]:
        return await asyncio.to_thread(self._get_data_sync, key)

    async def update_data(self, key: StorageKey, data: Mapping[str, Any]) -> Dict[str, Any]:
        if not isinstance(data, dict):
            raise DataNotDictLikeError(f"Data must be a dict or dict-like object, got {type(data).__name__}")
        return await asyncio.to_thread(self._update_data_sync, key, data.copy())

    async def close(self) -> None:
        pass

    def _set_state_sync(self, key, state_value):
        conn = self.conn_factory()
        cur = conn.cursor()
        try:
            params = self.key_params(key)
            cur.execute("""
                INSERT INTO aiogram_fsm_states (
                    bot_id, chat_id, user_id, thread_id, business_connection_id, destiny,
                    state, data_json, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, '{}'::jsonb, NOW())
                ON CONFLICT (bot_id, chat_id, user_id, thread_id, business_connection_id, destiny)
                DO UPDATE SET state = EXCLUDED.state,
                              updated_at = NOW()
            """, params + (state_value,))
            self._delete_empty_record(cur, params)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def _get_state_sync(self, key):
        conn = self.conn_factory()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT state
                FROM aiogram_fsm_states
                WHERE bot_id = %s
                  AND chat_id = %s
                  AND user_id = %s
                  AND thread_id = %s
                  AND business_connection_id = %s
                  AND destiny = %s
            """, self.key_params(key))
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            cur.close()
            conn.close()

    def _set_data_sync(self, key, data_json):
        conn = self.conn_factory()
        cur = conn.cursor()
        try:
            params = self.key_params(key)
            cur.execute("""
                INSERT INTO aiogram_fsm_states (
                    bot_id, chat_id, user_id, thread_id, business_connection_id, destiny,
                    state, data_json, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, NOW())
                ON CONFLICT (bot_id, chat_id, user_id, thread_id, business_connection_id, destiny)
                DO UPDATE SET data_json = EXCLUDED.data_json,
                              updated_at = NOW()
            """, params + (Json(data_json),))
            self._delete_empty_record(cur, params)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def _get_data_sync(self, key):
        conn = self.conn_factory()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT data_json
                FROM aiogram_fsm_states
                WHERE bot_id = %s
                  AND chat_id = %s
                  AND user_id = %s
                  AND thread_id = %s
                  AND business_connection_id = %s
                  AND destiny = %s
            """, self.key_params(key))
            row = cur.fetchone()
            return decode_fsm_data(row[0]) if row else {}
        finally:
            cur.close()
            conn.close()

    def _update_data_sync(self, key, data):
        conn = self.conn_factory()
        cur = conn.cursor()
        try:
            params = self.key_params(key)
            cur.execute("""
                INSERT INTO aiogram_fsm_states (
                    bot_id, chat_id, user_id, thread_id, business_connection_id, destiny,
                    state, data_json, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, NULL, '{}'::jsonb, NOW())
                ON CONFLICT (bot_id, chat_id, user_id, thread_id, business_connection_id, destiny)
                DO NOTHING
            """, params)
            cur.execute("""
                SELECT data_json
                FROM aiogram_fsm_states
                WHERE bot_id = %s
                  AND chat_id = %s
                  AND user_id = %s
                  AND thread_id = %s
                  AND business_connection_id = %s
                  AND destiny = %s
                FOR UPDATE
            """, params)
            row = cur.fetchone()
            current_data = decode_fsm_data(row[0]) if row else {}
            current_data.update(data)
            cur.execute("""
                UPDATE aiogram_fsm_states
                SET data_json = %s,
                    updated_at = NOW()
                WHERE bot_id = %s
                  AND chat_id = %s
                  AND user_id = %s
                  AND thread_id = %s
                  AND business_connection_id = %s
                  AND destiny = %s
            """, (Json(current_data),) + params)
            self._delete_empty_record(cur, params)
            conn.commit()
            return current_data.copy()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def _delete_empty_record(self, cur, params):
        cur.execute("""
            DELETE FROM aiogram_fsm_states
            WHERE bot_id = %s
              AND chat_id = %s
              AND user_id = %s
              AND thread_id = %s
              AND business_connection_id = %s
              AND destiny = %s
              AND state IS NULL
              AND data_json = '{}'::jsonb
        """, params)
