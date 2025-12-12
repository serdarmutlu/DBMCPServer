
from fastmcp.server.context import Context
import inspect

def inspect_sig():
    print(f"Signature: {inspect.signature(Context)}")
    print(f"Init Signature: {inspect.signature(Context.__init__)}")

if __name__ == "__main__":
    inspect_sig()
