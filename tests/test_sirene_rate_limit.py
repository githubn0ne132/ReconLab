import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from app.sirene import SireneClient, RateLimitExceeded
import httpx

@pytest.mark.anyio
async def test_rate_limit_exceeded():
    client = SireneClient("fake_token")

    # Mock response
    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_response.headers = {"Retry-After": "5"}

    # Mock AsyncClient.get
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response

        with pytest.raises(RateLimitExceeded) as excinfo:
            await client.get_by_siret("12345678901234")

        assert excinfo.value.retry_after == 5

@pytest.mark.anyio
async def test_rate_limit_default_retry():
    client = SireneClient("fake_token")

    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_response.headers = {} # No Retry-After

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response

        with pytest.raises(RateLimitExceeded) as excinfo:
            await client.get_by_siret("12345678901234")

        assert excinfo.value.retry_after == 60 # Default
