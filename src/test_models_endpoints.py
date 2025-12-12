import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import asyncio
from dbmcp.routes.models_routes import get_ollama_status, get_lmstudio_status, ServerStatus, ModelInfo
from starlette.requests import Request

class TestModelsEndpoints(unittest.IsolatedAsyncioTestCase):

    async def test_get_ollama_status_up(self):
        # Mock httpx.AsyncClient
        mock_response_root = MagicMock()
        mock_response_root.status_code = 200

        mock_response_tags = MagicMock()
        mock_response_tags.status_code = 200
        mock_response_tags.json.return_value = {
            "models": [
                {"name": "llama3:latest", "details": {"size": 4700000000}}
            ]
        }

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            # Side effect for consecutive calls: first for root, second for tags
            mock_client.get.side_effect = [mock_response_root, mock_response_tags]
            mock_client_cls.return_value = mock_client

            result = await get_ollama_status()

            self.assertIsInstance(result, ServerStatus)
            self.assertEqual(result.status, "UP")
            self.assertEqual(len(result.models), 1)
            self.assertEqual(result.models[0].name, "llama3:latest")
            self.assertEqual(result.models[0].details, {"size": 4700000000})

    async def test_get_ollama_status_down(self):
        mock_response_root = MagicMock()
        mock_response_root.status_code = 500

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.get.return_value = mock_response_root
            mock_client_cls.return_value = mock_client

            result = await get_ollama_status()

            self.assertEqual(result.status, "DOWN")
            self.assertIn("Status code: 500", result.error)

    async def test_get_lmstudio_status_up(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {"id": "mistral-instruct", "owned_by": "organization-owner"}
            ]
        }

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            result = await get_lmstudio_status()

            self.assertEqual(result.status, "UP")
            self.assertEqual(len(result.models), 1)
            self.assertEqual(result.models[0].name, "mistral-instruct")
            self.assertEqual(result.models[0].details, {"owned_by": "organization-owner"})

    async def test_get_lmstudio_status_down(self):
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            result = await get_lmstudio_status()

            self.assertEqual(result.status, "DOWN")
            self.assertIn("Status code: 404", result.error)

if __name__ == "__main__":
    unittest.main()
