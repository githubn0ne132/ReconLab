import httpx
from typing import Dict, Any, List, Optional
from loguru import logger
import asyncio

class SireneClient:
    BASE_URL = "https://api.insee.fr/api-sirene/3.11"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.headers = {
            "X-INSEE-Api-Key-Integration": api_key,
            "Accept": "application/json"
        } if api_key else {}

    def flatten_json(self, y: Dict, parent_key: str = '', sep: str = '.') -> Dict:
        """
        Flattens a nested dictionary.
        """
        items = []
        for k, v in y.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_json(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    async def check_connection(self) -> bool:
        """
        Checks connectivity to the SIRENE API using the /informations endpoint.
        """
        url = f"{self.BASE_URL}/informations"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=self.headers, timeout=5.0)
                if response.status_code == 200:
                    return True
                logger.error(f"API Connection Check Failed: {response.status_code} {response.text}")
                return False
        except Exception as e:
            logger.error(f"API Connection Check Exception: {e}")
            return False

    async def get_by_siret(self, siret: str) -> Optional[Dict[str, Any]]:
        """
        Fetches establishment data by SIRET. Returns a flattened dictionary.
        """
        url = f"{self.BASE_URL}/siret/{siret}"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=self.headers, timeout=10.0)

                if response.status_code == 200:
                    data = response.json()
                    # The API returns wrapper like {"etablissement": {...}, "header": ...}
                    # We are interested in "etablissement"
                    if "etablissement" in data:
                        return self.flatten_json(data["etablissement"])
                    return self.flatten_json(data) # Fallback
                elif response.status_code == 404:
                    logger.warning(f"SIRET {siret} not found. {response.text}")
                    return None
                elif response.status_code == 429:
                    logger.warning("Rate limit exceeded.")
                    return None
                else:
                    logger.error(f"API Error {response.status_code}: {response.text}")
                    return None
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return None

    def get_common_fields(self) -> List[str]:
        """
        Returns a list of common fields for mapping suggestion.
        """
        return [
            "siret",
            "uniteLegale.denominationUniteLegale",
            "adresseEtablissement.numeroVoieEtablissement",
            "adresseEtablissement.typeVoieEtablissement",
            "adresseEtablissement.libelleVoieEtablissement",
            "adresseEtablissement.codePostalEtablissement",
            "adresseEtablissement.libelleCommuneEtablissement",
            "etablissement.etatAdministratifEtablissement"
        ]
