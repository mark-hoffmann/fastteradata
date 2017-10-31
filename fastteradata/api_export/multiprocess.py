
import sys
import subprocess

def call_sub(f):
    print(f"Calling Fast Export on file...  {f}")
    sys.stdout.flush()
    subprocess.call(f"fexp < {f}", shell=True)
    sys.stdout.flush()
    return("")
