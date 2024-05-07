# Daša Nosková - xnosko05
# VUT FIT 2024

import asyncio
import traceback
import json
import sys
import argparse
from ServerWS import ServerWS


async def main(args):
    ws = ServerWS(kafka_topic=args['kafka_topic'], bootstrap_servers=args['bootstrap_servers'], host=args['host'], port=args['port'])

    try:
        await ws.run()
    except Exception:
        traceback.print_exc()
    finally:
        print("WebSocket server stopped.")
        sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run websocket server')
    parser.add_argument('--config', type=str,  help='Json config file.', default="config.json")
    args = parser.parse_args()

    with open(args.config, 'r') as config_file:
        config = json.load(config_file)

    asyncio.get_event_loop().run_until_complete(main(config))
