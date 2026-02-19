import time

from .validator import Validator


def main() -> None:
    validator = Validator()
    with validator.running():
        print("Event bus and actors are running. Press Ctrl+C to stop.")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
