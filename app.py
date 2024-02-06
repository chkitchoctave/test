import signal
import sys
from types import FrameType
from flask import Flask
from utils.logging import logger, flush  # Import flush function
import subprocess


app = Flask(__name__)


@app.route("/")
def hello() -> str:
    # Use basic logging with custom fields
    logger.info(logField="custom-entry", arbitraryField="custom-entry")

    # https://cloud.google.com/run/docs/logging#correlate-logs
    logger.info("Child logger with trace Id.")

    return "Hello, Chrissy World!"


def run_python_script(script_path):
    try:
        subprocess.run(['python', script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")


def shutdown_handler(signal_int: int, frame: FrameType) -> None:
    logger.info(f"Caught Signal {signal.strsignal(signal_int)}")

    flush()  # Call flush function

    # Safely exit program
    sys.exit(0)


if __name__ == "__main__":
    # Running application locally, outside of a Google Cloud Environment

    # handles Ctrl-C termination
    signal.signal(signal.SIGINT, shutdown_handler)

    run_python_script("/test/ck.py")  # Provide script path

    app.run(host="localhost", port=8080, debug=True)
else:
    # handles Cloud Run container termination
    signal.signal(signal.SIGTERM, shutdown_handler)
