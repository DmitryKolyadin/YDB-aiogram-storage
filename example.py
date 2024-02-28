import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

import ydb
from aiogram_ydb_storage import YDBStorage

driver_config = ydb.DriverConfig(
    "grpcs://...",
    "...",
    credentials=ydb.credentials_from_env_variables(),
)

my_storage = YDBStorage(driver_config=driver_config)

dp = Dispatcher(storage=my_storage)
bot = Bot("token")


class st(StatesGroup):
    test1 = State()
    test1 = State()


@dp.message(CommandStart())
async def command_start_handler(message: Message, state: FSMContext) -> None:
    await state.set_state(st.test1)
    await message.answer("Set state test1")


@dp.message(st.test1)
async def handler(message: types.Message, state: FSMContext) -> None:
    await message.answer("state test1. Set state test2")
    await state.set_state(st.test2)


@dp.message()
async def echo_handler(message: types.Message, state: FSMContext) -> None:
    await message.answer("not state test1. Set state test1")
    await state.set_state(st.test1)


async def main() -> None:
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
