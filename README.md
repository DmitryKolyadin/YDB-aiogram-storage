# aiogram YDB Storage

`aiogram_ydb_storage` is a storage implementation for `aiogram FSM` that utilizes Yandex Database (YDB) as the storage backend. This allows you to persist state data for state machines used in your Telegram bots.

## Installation

You can install `aiogram_ydb_storage` via pip:

```bash
pip install aiogram_ydb_storage
```

## Usage

To use `aiogram_ydb_storage`, you need to first set up your YDB database and obtain necessary credentials. Then, you can use it in conjunction with `aiogram` as follows:

```python
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

import ydb
from aiogram_ydb_storage import YDBStorage

# Configure YDB driver
driver_config = ydb.DriverConfig(
    "grpcs://...",  # YDB endpoint
    "...",          # Database name
    credentials=ydb.credentials_from_env_variables(),  # Use YDB credentials from environment
)

# Initialize YDBStorage
my_storage = YDBStorage(driver_config=driver_config)

# Initialize aiogram Bot and Dispatcher
dp = Dispatcher(storage=my_storage)
bot = Bot("token")

# Define your states
class MyStates(StatesGroup):
    test1 = State()
    test2 = State()

# Handlers
@dp.message(CommandStart())
async def command_start_handler(message: Message, state: FSMContext) -> None:
    await state.set_state(MyStates.test1)
    await message.answer("Set state test1")

@dp.message(MyStates.test1)
async def handler(message: types.Message, state: FSMContext) -> None:
    await message.answer("state test1. Set state test2")
    await state.set_state(MyStates.test2)

@dp.message()
async def echo_handler(message: types.Message, state: FSMContext) -> None:
    await message.answer("not state test1. Set state test1")
    await state.set_state(MyStates.test1)

# Main function
async def main() -> None:
    await dp.start_polling(bot)

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
```

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.