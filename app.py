import signal
import sys
from types import FrameType
from flask import Flask
from utils.logging import logger, flush  # Import flush function
import subprocess


app = Flask(__name__)

@app.route("/")
def hello() -> str:
    return "Hello, Chrissy World!"

@app.route("/")
def run_script() -> str:
    # Run your Python script
    try:
        subprocess.run(["python", "test/ck.py"], check=True)
        return "Script executed successfully"
    except subprocess.CalledProcessError as e:
        return f"Error executing script: {e}"

if __name__ == "__main__":
    app.run(debug=True)
