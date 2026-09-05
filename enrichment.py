from __future__ import annotations

import logging

from finflux.config import Settings
from finflux.exceptions import TransientProcessingError
from finflux.models.cdc import NormalizedTransaction

logger = logging.getLogger(__name__)

REGIONS = {
    "MUMBAI": "WEST",
    "PUNE": "WEST",
    "DELHI": "NORTH",
    "BENGALURU": "SOUTH",
    "BANGALORE": "SOUTH",
    "CHENNAI": "SOUTH",
    "KOLKATA": "EAST",
    "HYDERABAD": "SOUTH",
}


def enrich(tx: NormalizedTransaction, settings: Settings) -> dict[str, str]:
    try:
        location = (tx.location or "").strip().upper()
        region = REGIONS.get(location, "UNKNOWN")
        device_risk = "NEW_DEVICE" if tx.device_id and "NEW" in tx.device_id.upper() else "KNOWN_DEVICE"
        return {"region": region, "deviceRisk": device_risk, "enrichmentStatus": "OK"}
    except Exception as ex:
        if not settings.enrichment_fail_open:
            raise TransientProcessingError("ENRICHMENT_FAILURE", "Enrichment failed") from ex
        logger.warning("Enrichment failed; continuing with fallback transactionId=%s", tx.transaction_id)
        return {"region": "UNKNOWN", "deviceRisk": "UNKNOWN", "enrichmentStatus": "FALLBACK"}
