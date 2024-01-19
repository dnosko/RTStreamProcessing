import asyncio

from consumer.Consumer import Consumer
import sys


async def main():
    consumer = Consumer()

    try:
        await consumer.run()

    finally:
        print("WebSocket server stopped.")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
