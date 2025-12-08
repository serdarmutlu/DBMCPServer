import json
from typing import Optional

from fastmcp.tools.tool import ToolResult
from mcp import types as mt

def text_result(payload: dict | list, title: Optional[str] = None) -> ToolResult:
    """JSON çıktıyı TextContent olarak döndürmek için yardımcı fonksiyon."""
    text = json.dumps(payload, indent=2, default=str)
    if title:
        text = f"# {title}\n\n```json\n{text}\n```"
    else:
        text = f"```json\n{text}\n```"

    return ToolResult(
        content=[
            mt.TextContent(type="text", text=text)
        ]
    )