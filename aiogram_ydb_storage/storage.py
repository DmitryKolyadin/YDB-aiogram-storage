import asyncio
from aiogram.fsm.storage.base import BaseStorage, StorageKey
from aiogram.fsm.state import State
from typing import Any, Dict, Optional

import ydb
import pickle
import json

import logging

logger = logging.getLogger(__name__)


class YDBStorage(BaseStorage):
    """YDB storage for FSM"""

    def __init__(
        self,
        driver_config: ydb.DriverConfig = None,
        session_pool: ydb.SessionPool = None,
        serializing_method: str = "json",
        table_name: str = "fsm_storage",
    ) -> None:

        # Database
        self.driver = ydb.Driver(driver_config) if driver_config else None
        # self.driver_ = ydb.aio.Driver(driver_config)

        self.session_pool = (
            ydb.SessionPool(self.driver) if session_pool is None else session_pool
        )

        # Settigns
        self.table_name = table_name

        self.serializing_method = serializing_method
        if self.serializing_method != "pickle" and self.serializing_method != "json":
            self.serializing_method = "json"

        # Create table
        try:
            asyncio.run(self._create_table())
        except BaseException as e:
            logger.error(f"FSM Storage error: {e}")

    def _key(self, key: StorageKey) -> str:
        """
        Create a key for every uniqe user, chat and bot
        """
        result_string = (
            str(key.bot_id) + ":" + str(key.chat_id) + ":" + str(key.user_id)
        )
        return result_string

    def _ser(self, obj: object) -> str | bytes | None:
        """
        Serialize object
        """
        try:
            match self.serializing_method:
                case "pickle":
                    return pickle.dumps(obj)
                case "json" | _:
                    return json.dumps(obj)
        except Exception as e:
            logger.error(f"Serializing error! {e}")
            return None

    def _dsr(self, obj) -> Optional[Dict[str, Any]]:
        """
        Deserialize object
        """
        try:
            match self.serializing_method:
                case "pickle":
                    return pickle.loads(obj) if obj else None
                case "json" | _:
                    return json.loads(obj) if obj else None
        except Exception as e:
            logger.error(
                f"Deserializing error! Probably, unsupported serializing method was used. {e}"
            )
            return None

    async def _execute_query(self, query: str, parameters: dict = None):
        """Execute YQL query"""

        async def callee(session_pool):
            with session_pool.async_checkout() as session_holder:
                try:
                    # wait for the session checkout to complete.
                    session = await asyncio.wait_for(
                        asyncio.wrap_future(session_holder), timeout=5
                    )
                except asyncio.TimeoutError:
                    raise ydb.SessionPoolEmpty("")

                prepared_query = session.prepare(query)

                return await asyncio.wrap_future(
                    session.transaction().async_execute(
                        prepared_query,
                        parameters,
                        commit_tx=True,
                        settings=ydb.BaseRequestSettings()
                        .with_timeout(3)
                        .with_operation_timeout(2),
                    )
                )

        return await ydb.aio.retry_operation(callee, None, self.session_pool)

    async def _create_table(self):
        """
        Create table if not exists
        """

        query = f"""
                CREATE table `{self.table_name}` (
                    `key` Utf8,
                    `data` Utf8,
                    `state` Utf8,
                    PRIMARY KEY (`key`)
                )
                """

        async def callee(session_pool):
            with session_pool.async_checkout() as session_holder:
                try:
                    # wait for the session checkout to complete.
                    session = await asyncio.wait_for(
                        asyncio.wrap_future(session_holder), timeout=5
                    )
                except asyncio.TimeoutError:
                    raise ydb.SessionPoolEmpty("")

                return await asyncio.wrap_future(session.execute_scheme(query))

        try:
            return await ydb.aio.retry_operation(callee, None, self.session_pool)
        except BaseException as e:
            logger.error(f"FSM Storage error: {e}")

    async def set_state(self, key: StorageKey, state: State | None = None) -> None:
        """
        Set state for specified key

        :param key: storage key
        :param state: new state
        """
        s_key = self._key(key)
        s_state = state.state if isinstance(state, State) else state
        s_state = s_state if s_state else ""

        try:
            query = f"""
                    DECLARE $k AS Utf8;
                    DECLARE $s AS Utf8;
                    UPSERT INTO `{self.table_name}` (`key`, `state`)
                    VALUES ($k, $s)
                    """
            await self._execute_query(
                query,
                {
                    "$k": s_key,
                    "$s": s_state,
                },
            )

        except BaseException as e:
            logger.error(f"FSM Storage error: {e}")

    async def get_state(self, key: StorageKey) -> Optional[str]:
        """
        Get key state

        :param key: storage key
        :return: current state
        """
        s_key = self._key(key)

        try:
            query = f"""
                    DECLARE $k AS Utf8;
                    SELECT state FROM `{self.table_name}`
                    WHERE `key` = $k
                    """
            result = await self._execute_query(
                query,
                {
                    "$k": s_key,
                },
            )

            return result[0].rows[0].get("state") if result[0].rows else None

        except BaseException as e:
            logger.error(f"FSM Storage error: {e}")

    async def set_data(self, key: StorageKey, data: Dict[str, Any]) -> None:
        """
        Write data (replace)

        :param key: storage key
        :param data: new data
        """
        s_key = self._key(key)
        s_data = self._ser(data)

        try:
            query = f"""
                    DECLARE $k AS Utf8;
                    DECLARE $d AS Utf8;
                    UPSERT INTO `{self.table_name}` (`key`, `data`)
                    VALUES ($k, $d)
                    """
            await self._execute_query(
                query,
                {
                    "$k": s_key,
                    "$d": s_data,
                },
            )
        except BaseException as e:
            logger.error(f"FSM Storage error: {e}")

    async def get_data(self, key: StorageKey) -> Optional[Dict[str, Any]]:
        """
        Get current data for key

        :param key: storage key
        :return: current data
        """
        s_key = self._key(key)

        try:
            query = f"""
                    DECLARE $k AS Utf8;
                    SELECT data FROM `{self.table_name}`
                    WHERE `key` = $k
                    """
            result = await self._execute_query(
                query,
                {
                    "$k": s_key,
                },
            )

            return self._dsr(result[0].rows[0].get("data")) if result[0].rows else None

        except BaseException as e:
            logger.error(f"FSM Storage error: {e}")
            return None

    async def update_data(
        self, key: StorageKey, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update date in the storage for key (like dict.update)

        :param key: storage key
        :param data: partial data
        :return: new data
        """
        current_data = await self.get_data(key=key)
        if not current_data:
            current_data = {}
        current_data.update(data)
        await self.set_data(key=key, data=current_data)
        return current_data.copy()

    async def close(self) -> None:
        """
        Close storage (database connection, file or etc.)
        """

        # logger.debug("FSM Storage database has been closed.")
        pass
