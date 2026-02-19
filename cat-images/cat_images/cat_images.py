import time

from .validator import Validator


def main() -> None:
    validator = Validator()
    validator.run_loop()

    print("Event bus and actors are running. Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        validator.stop_and_wait_for_shutdown()


if __name__ == "__main__":
    main()
