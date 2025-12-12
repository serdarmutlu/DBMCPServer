from starlette.requests import Request
from starlette.responses import JSONResponse
from fastmcp import FastMCP
import asyncio
from typing import List, Dict, Any

def register_introspection_routes(mcpserver: FastMCP):
    """
    Registers introspection endpoints to list tools, resources, and prompts.
    """

    @mcpserver.custom_route("/metadata/tools", methods=["GET"])
    async def list_tools(request: Request):
        """Returns a list of available tools with their details."""
        tools_data = []
        
        # Access the internal tool manager to get the actual tool objects
        # We use the internal _tool_manager as get_tools() might return simplified objects or strings depending on version/context
        # But based on our inspection, get_tools() returns a dict of tools or list.
        # Let's try to use the public API first if possible, or fall back to internal structure if needed.
        # In the inspection script, await server.get_tools() returned a dict.
        
        try:
            tools = await mcpserver.get_tools() if asyncio.iscoroutinefunction(mcpserver.get_tools) else mcpserver.get_tools()
            
            # Tools can be a dict (name -> tool) or list of tools
            tool_list = []
            if isinstance(tools, dict):
                tool_list = tools.values()
            elif isinstance(tools, list):
                tool_list = tools
            
            for tool in tool_list:
                # Extract relevant information
                tool_info = {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": []
                }
                
                # Extract parameters from inputSchema or parameters attribute
                # FastMCP tools often have 'parameters' attribute with JSON schema
                params_schema = getattr(tool, "parameters", None)
                if not params_schema:
                     params_schema = getattr(tool, "inputSchema", None)
                
                if params_schema and "properties" in params_schema:
                    for param_name, param_details in params_schema["properties"].items():
                        tool_info["parameters"].append({
                            "name": param_name,
                            "type": param_details.get("type", "unknown"),
                            "description": param_details.get("description", ""),
                            "default": param_details.get("default"),
                            "required": param_name in params_schema.get("required", [])
                        })
                
                tools_data.append(tool_info)
                
            return JSONResponse(content=tools_data)
        except Exception as e:
             return JSONResponse(content={"error": str(e)}, status_code=500)

    @mcpserver.custom_route("/metadata/resources", methods=["GET"])
    async def list_resources(request: Request):
        try:
            # get_resources returns a list of resource objects/strings
            resources = await mcpserver.get_resources() if asyncio.iscoroutinefunction(mcpserver.get_resources) else mcpserver.get_resources()
            
            # Since get_resources might return just registered paths or objects, we'll format them consistently
            resources_data = []
            for res in resources:
                # Inspecting the resource object structure from previous knowledge or assuming standard attrs
                # If it's a simple object, we dump it. 
                # Based on FastMCP, it might be a Resource object with uri, name, description
                res_info = {
                    "uri": getattr(res, "uri", str(res)),
                    "name": getattr(res, "name", None),
                    "description": getattr(res, "description", None),
                    "mimeType": getattr(res, "mimeType", None)
                }
                resources_data.append(res_info)
                
            return JSONResponse(content=resources_data)
        except Exception as e:
            return JSONResponse(content={"error": str(e)}, status_code=500)

    @mcpserver.custom_route("/metadata/prompts", methods=["GET"])
    async def list_prompts(request: Request):
        try:
            prompts = await mcpserver.get_prompts() if asyncio.iscoroutinefunction(mcpserver.get_prompts) else mcpserver.get_prompts()
            
            prompts_data = []
            for prompt in prompts:
                prompt_info = {
                    "name": getattr(prompt, "name", str(prompt)),
                    "description": getattr(prompt, "description", None),
                    "arguments": []
                }
                
                # Extract arguments if available
                arguments = getattr(prompt, "arguments", [])
                if arguments:
                    for arg in arguments:
                        prompt_info["arguments"].append({
                            "name": getattr(arg, "name", str(arg)),
                            "description": getattr(arg, "description", None),
                            "required": getattr(arg, "required", False)
                        })

                prompts_data.append(prompt_info)

            return JSONResponse(content=prompts_data)
        except Exception as e:
            return JSONResponse(content={"error": str(e)}, status_code=500)

    @mcpserver.custom_route("/metadata/tools/execute", methods=["POST"])
    async def list_tool_execute(request: Request):
        try:
            data = await request.json()
            tool_name = data.get("name")
            arguments = data.get("arguments", {})
            
            if not tool_name:
                 return JSONResponse(content={"error": "Tool name is required"}, status_code=400)
            
            tool = await mcpserver.get_tool(tool_name) if asyncio.iscoroutinefunction(mcpserver.get_tool) else mcpserver.get_tool(tool_name)
            
            if not tool:
                return JSONResponse(content={"error": f"Tool '{tool_name}' not found"}, status_code=404)
            
            # Execute tool
            try:
                # Inject context
                from fastmcp.server.context import Context, _current_context
                ctx = Context(fastmcp=mcpserver)
                token = _current_context.set(ctx)
                try:
                    result = await tool.run(arguments)
                finally:
                    _current_context.reset(token)
                
                # Format result
                response_content = []
                if hasattr(result, 'content'):
                    for item in result.content:
                         if hasattr(item, 'text'):
                             response_content.append({"type": "text", "text": item.text})
                         else:
                             # Serializing other types might be needed
                             response_content.append({"type": "unknown", "content": str(item)})
                else:
                     response_content = str(result)
                     
                return JSONResponse(content={"result": response_content})
                
            except Exception as e:
                 return JSONResponse(content={"error": f"Tool execution failed: {str(e)}"}, status_code=500)

        except Exception as e:
            return JSONResponse(content={"error": str(e)}, status_code=500)
