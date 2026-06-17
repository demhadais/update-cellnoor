import asyncio

from pydantic_settings import CliApp

from app import Cli


async def main() -> None:
    _ = CliApp.run(Cli)


if __name__ == "__main__":
    asyncio.run(main())
