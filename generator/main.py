import Generator
import os
def main():

    generator = Generator()

    try:
        generator.hello()
        generator.send_data()
    finally:
        generator.stop()
        os.exit(0)


if __name__ == "__main__":
    main()