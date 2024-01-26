import asyncio

from producer.ServerWS import ServerWS
import sys


async def main():
    ws = ServerWS()

    try:
        await ws.run()

    finally:
        print("WebSocket server stopped.")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
