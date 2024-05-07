# Daša Nosková - xnosko05
# VUT FIT 2024

from Generator import Generator

import gzip

import argparse
import asyncio
async def main(args):

    generator = Generator(devices=args.num_dev, limit_x=args.limit_x, limit_y=args.limit_y, uri=args.ws, limit_cnt=args.limit)
    # Generate data to file (for testing)
    if args.file:
        if args.limit is None:
            print("Limit is required with writing to file.")
            exit(0)
        else:
            # use compressed gz file
            gz_file_name = args.file if args.file.endswith('.gz') else f"{args.file}.gz"
            with gzip.open(gz_file_name, 'wt') as file:
                for data in generator.gen_data(args.limit):
                    file.write(data + "\n")
    else:
        try:
            await generator.hello()
            await generator.send_data()
        finally:
            await generator.stop()
            asyncio.get_event_loop().stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run mock websocket client data generator.')

    parser.add_argument('--num_dev', type=int, default=1000, help='Number of devices. If not specified, its 1000.', required=False)
    parser.add_argument('--limit_x', type=str, default='0.0,100.0', help='Longitude interval. If not specified, its 0-100.0', required=False)
    parser.add_argument('--limit_y', type=str, default='0.0,100.0', help='Latitude interval. If not specified, its 0-100.0', required=False)
    parser.add_argument('--ws', type=str, default='ws://localhost:8088/ws', help='Websocket connection uri. If not specified, "ws://localhost:8088/ws" is used.', required=False)
    parser.add_argument('--limit', type=int, help='Limit number of generated records.', required=False)
    parser.add_argument('--file', type=str, required=False)

    args = parser.parse_args()
    # Convert limit_x and limit_y from string to tuple
    args.limit_x = tuple(map(float, args.limit_x.split(',')))
    args.limit_y = tuple(map(float, args.limit_y.split(',')))

    asyncio.run(main(args))