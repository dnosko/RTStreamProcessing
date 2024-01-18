import asyncio

import Consumer
import os

async def main():

    consumer = Consumer()

    try:
        await consumer.start_websocket()

    finally:
        consumer.stop()
        os.exit(0)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())