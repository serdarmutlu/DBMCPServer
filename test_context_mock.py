
from fastmcp import FastMCP
from fastmcp.server.context import Context, _current_context
import asyncio

# Mock session
class MockSession:
    async def send_log_message(self, *args, **kwargs):
        pass
    async def send_tool_list_changed(self):
        pass

def test_context_mocking():
    server = FastMCP(name="Test")
    
    @server.tool()
    async def add(a: int, b: int) -> int:
        return a + b

    async def run_checks():
        # Create dummy context
        # Check constructor using help or inspection
        try:
             # Try minimal args. From source reading earlier, it has session.
             # but I didn't see __init__ signature. 
             # Let's inspect signature
             import inspect
             print(f"Context Sig: {inspect.signature(Context.__init__)}")
        except Exception as e:
             print(f"Inspect failed: {e}")

        # Assuming Context(session, request_id, client_id=None, fastmcp=None)
        # We'll try to mock it.
        
        ctx = Context(session=MockSession(), request_id="test", client_id="test_client", fastmcp=server)
        token = _current_context.set(ctx)
        
        try:
             tool = await server.get_tool("add") if asyncio.iscoroutinefunction(server.get_tool) else server.get_tool("add")
             result = await tool.run({"a": 1, "b": 2})
             print(f"Result with context: {result}")
        except Exception as e:
             print(f"Run failed: {e}")
             import traceback
             traceback.print_exc()
        finally:
             _current_context.reset(token)

    asyncio.run(run_checks())

if __name__ == "__main__":
    test_context_mocking()
