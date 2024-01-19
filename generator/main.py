from Generator import Generator
import sys
import asyncio
async def main():

    generator = Generator()

    try:
        await generator.hello()
        await generator.send_data()
    finally:
        await generator.stop()
        asyncio.get_event_loop().stop()


if __name__ == "__main__":
    asyncio.run(main())