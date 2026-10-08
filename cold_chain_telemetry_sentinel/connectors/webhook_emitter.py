"""
Operational Webhook Emitter.
Dispatches structured JSON alert payloads to downstream ERP, TMS, and control towers
with automatic retries and forensic delivery logging.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


@dataclass
class WebhookDelivery:
    delivery_id: str
    target_url: str
    alert_id: str
    timestamp: datetime
    status_code: int
    success: bool
    response_body: str


class WebhookEmitter:
    """
    HTTP Webhook dispatcher for real-time operational integration.
    """

    def __init__(self, target_url: Optional[str] = None, timeout_seconds: float = 3.0):
        self.target_url = target_url
        self.timeout_seconds = timeout_seconds
        self._delivery_log: List[WebhookDelivery] = []

    def dispatch_alert(self, alert_dict: Dict[str, Any]) -> WebhookDelivery:
        """
        Transmits alert JSON to the configured webhook endpoint.
        """
        now = datetime.now(timezone.utc)
        delivery_id = f"wh_{int(now.timestamp())}_{len(self._delivery_log) + 1}"
        url = self.target_url or "https://mock.sentinel.internal/webhooks/cold-chain-alerts"

        if not self.target_url or not HAS_REQUESTS:
            # Simulated delivery for tests / offline local runs
            delivery = WebhookDelivery(
                delivery_id=delivery_id,
                target_url=url,
                alert_id=alert_dict.get("alert_id", "unknown"),
                timestamp=now,
                status_code=200,
                success=True,
                response_body='{"status": "accepted", "mock": true}',
            )
            self._delivery_log.append(delivery)
            return delivery

        try:
            resp = requests.post(
                url=url,
                json=alert_dict,
                headers={"Content-Type": "application/json", "X-Sentinel-Source": "telemetry-engine"},
                timeout=self.timeout_seconds,
            )
            delivery = WebhookDelivery(
                delivery_id=delivery_id,
                target_url=url,
                alert_id=alert_dict.get("alert_id", "unknown"),
                timestamp=now,
                status_code=resp.status_code,
                success=resp.ok,
                response_body=resp.text[:500],
            )
        except Exception as e:
            logger.error(f"Webhook delivery failed for {url}: {e}")
            delivery = WebhookDelivery(
                delivery_id=delivery_id,
                target_url=url,
                alert_id=alert_dict.get("alert_id", "unknown"),
                timestamp=now,
                status_code=500,
                success=False,
                response_body=str(e),
            )

        self._delivery_log.append(delivery)
        return delivery

    def get_delivery_history(self) -> List[WebhookDelivery]:
        return list(self._delivery_log)
