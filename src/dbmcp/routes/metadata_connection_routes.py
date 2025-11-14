from pydantic import BaseModel, ValidationError

from starlette.requests import Request
from starlette.responses import JSONResponse
from db.encryption import encrypt_password


def register_connection_routes(mcpserver, metadata_manager, db_manager):

    # --- Database Types ---
    @mcpserver.custom_route("/metadata/database-types", methods=["GET"])
    async def list_database_types(request: Request):
        rows = await metadata_manager.get_all_types()
        return JSONResponse(content=[dict(r) for r in rows])

    @mcpserver.custom_route("/metadata/database-types/{id:int}", methods=["GET"])
    async def list_database_types(request: Request):
        id = int(request.path_params["id"])
        row = await metadata_manager.get_type(id)
        return JSONResponse(content=dict(row) if row else {"error": "Not found"}, status_code=200 if row else 404)

    @mcpserver.custom_route("/metadata/database-types", methods=["POST"])
    async def create_database_type(request: Request):
        data = await request.json()
        row = await metadata_manager.add_type(data["name"])
        return JSONResponse(content=dict(row))

    @mcpserver.custom_route("/metadata/database-types/{id:int}", methods=["PUT"])
    async def update_type(request: Request):
        id = int(request.path_params["id"])
        data = await request.json()
        row = await metadata_manager.update_type(id, data)
        return JSONResponse(content=dict(row))

    @mcpserver.custom_route("/metadata/database-types/{id:int}", methods=["DELETE"])
    async def delete_type(request: Request):
        id = int(request.path_params["id"])
        await metadata_manager.delete_type(id)
        return JSONResponse(content={"status": "deleted"})



    # --- Database Connections ---
    @mcpserver.custom_route("/metadata/database-connections", methods=["GET"])
    async def list_connections(request: Request):
        rows = await metadata_manager.get_all_connections()
        return JSONResponse(content=[dict(r) for r in rows])

    @mcpserver.custom_route("/metadata/database-connections/{connection_id:int}", methods=["GET"])
    async def get_connection(request: Request):
        connection_id = int(request.path_params["connection_id"])
        row = await metadata_manager.get_connection(connection_id)
        return JSONResponse(content=dict(row) if row else {"error": "Not found"}, status_code=200 if row else 404)


    @mcpserver.custom_route("/metadata/database-connections", methods=["POST"])
    async def add_connection(request: Request):
        data = await request.json()
        password = data["password"]
        encrypted_password = encrypt_password(password)
        data["encrypted_password"] = encrypted_password
        row = await metadata_manager.add_connection(data)
        return JSONResponse(content=dict(row))


    @mcpserver.custom_route("/metadata/database-connections/{connection_id:int}", methods=["PUT"])
    async def update_connection(request: Request):
        connection_id = int(request.path_params["connection_id"])
        data = await request.json()
        if "password" in data:
            data["encrypted_password"] = encrypt_password(data["password"])
            row = await metadata_manager.update_connection(connection_id, data)
        else:
            row = await metadata_manager.update_connection_no_password(connection_id, data)

        return JSONResponse(content=dict(row))

    @mcpserver.custom_route("/metadata/database-connections/{connection_id:int}", methods=["DELETE"])
    async def delete_connection(request: Request):
        connection_id = int(request.path_params["connection_id"])
        await metadata_manager.delete_connection(connection_id)
        return JSONResponse(content={"status": "deleted"})




    # --- 1️⃣ Tüm bağlantıları pasifleştirme ---
    @mcpserver.custom_route("/metadata/database-connections/deactivate-all", methods=["POST"])
    async def deactivate_all_connections(request):
        """Tüm bağlantıları pasifleştirir (is_active = FALSE)."""
        try:
            result = await metadata_manager.deactivate_all_connections()
            return JSONResponse(content=result)
        except Exception as e:
            return JSONResponse(
                content={"status": "error", "message": f"Failed to deactivate all connections: {e}"},
                status_code=500
            )


    # --- 2️⃣ Belirli bağlantıyı aktifleştirme ---
    @mcpserver.custom_route("/metadata/database-connections/{connection_id:int}/activate", methods=["POST"])
    async def activate_connection(request):
        """Belirli bir bağlantıyı aktif hale getirir (is_active = TRUE)."""
        try:
            connection_id = request.path_params["connection_id"]
            result = await metadata_manager.activate_connection(connection_id)
            status = 200 if result.get("status", "ok") == "ok" or "id" in result else 404
            return JSONResponse(content=result, status_code=status)
        except Exception as e:
            return JSONResponse(
                content={"status": "error", "message": f"Failed to activate connection: {e}"},
                status_code=500
            )


    # --- 3️⃣ Belirli bağlantıyı pasifleştirme ---
    @mcpserver.custom_route("/metadata/database-connections/{connection_id:int}/deactivate", methods=["POST"])
    async def deactivate_connection(request):
        """Belirli bir bağlantıyı pasif hale getirir (is_active = FALSE)."""
        try:
            connection_id = request.path_params["connection_id"]
            result = await metadata_manager.deactivate_connection(connection_id)
            status = 200 if result.get("status", "ok") == "ok" or "id" in result else 404
            return JSONResponse(content=result, status_code=status)
        except Exception as e:
            return JSONResponse(
                content={"status": "error", "message": f"Failed to deactivate connection: {e}"},
                status_code=500
            )
