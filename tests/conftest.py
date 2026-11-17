import os
import sys

# Spark launches Python workers as subprocesses. Without this, Windows resolves
# the bare ``python`` command to the Microsoft Store alias instead of the
# interpreter running the tests.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
