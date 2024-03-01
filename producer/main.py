import asyncio
import traceback

from ServerWS import ServerWS
import sys


async def main():
    ws = ServerWS()

    try:
        await ws.run()
    except Exception:
        traceback.print_exc()
    finally:
        print("WebSocket server stopped.")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
