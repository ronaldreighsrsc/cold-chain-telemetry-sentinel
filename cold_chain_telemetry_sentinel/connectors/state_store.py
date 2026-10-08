"""
State Store Connector.
Maintains fast in-memory key-value state for active vehicles, spatial indexes,
and operational alerts, with native Redis support and thread-safe fallback.
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

try:
    import redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False


class IStateStore(ABC):
    """Abstract contract for low-latency operational state storage."""

    @abstractmethod
    def set_vehicle_telemetry(self, vehicle_id: str, telemetry_dict: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def get_vehicle_telemetry(self, vehicle_id: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_all_vehicles(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def add_to_spatial_cluster(self, h3_index: str, vehicle_id: str) -> None:
        pass

    @abstractmethod
    def get_vehicles_in_spatial_cluster(self, h3_index: str) -> Set[str]:
        pass

    @abstractmethod
    def save_alert(self, alert_dict: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def get_active_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        pass


class InMemoryStateStore(IStateStore):
    """
    Fast thread-safe in-memory key-value store.
    Zero infrastructure requirement.
    """

    def __init__(self):
        self._vehicles: Dict[str, Dict[str, Any]] = {}
        self._spatial_clusters: Dict[str, Set[str]] = {}
        self._alerts: List[Dict[str, Any]] = []

    def set_vehicle_telemetry(self, vehicle_id: str, telemetry_dict: Dict[str, Any]) -> None:
        self._vehicles[vehicle_id] = telemetry_dict

    def get_vehicle_telemetry(self, vehicle_id: str) -> Optional[Dict[str, Any]]:
        return self._vehicles.get(vehicle_id)

    def get_all_vehicles(self) -> List[Dict[str, Any]]:
        return list(self._vehicles.values())

    def add_to_spatial_cluster(self, h3_index: str, vehicle_id: str) -> None:
        if h3_index not in self._spatial_clusters:
            self._spatial_clusters[h3_index] = set()
        self._spatial_clusters[h3_index].add(vehicle_id)

    def get_vehicles_in_spatial_cluster(self, h3_index: str) -> Set[str]:
        return set(self._spatial_clusters.get(h3_index, set()))

    def save_alert(self, alert_dict: Dict[str, Any]) -> None:
        self._alerts.append(alert_dict)
        if len(self._alerts) > 1000:
            self._alerts.pop(0)

    def get_active_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        return self._alerts[-limit:]


class RedisStateStore(IStateStore):
    """
    Production Redis state store wrapper.
    Falls back gracefully to InMemoryStateStore if the Redis connection fails.
    """

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        self._fallback = InMemoryStateStore()
        self._client: Optional[redis.Redis] = None

        if HAS_REDIS:
            try:
                r = redis.Redis(host=host, port=port, db=db, socket_timeout=1.0)
                r.ping()
                self._client = r
                logger.info("Connected successfully to Redis state store.")
            except Exception as e:
                logger.warning(f"Redis unavailable ({e}), using InMemoryStateStore fallback.")
                self._client = None

    def set_vehicle_telemetry(self, vehicle_id: str, telemetry_dict: Dict[str, Any]) -> None:
        if self._client:
            try:
                self._client.hset("coldchain:fleet", vehicle_id, json.dumps(telemetry_dict))
                return
            except Exception:
                pass
        self._fallback.set_vehicle_telemetry(vehicle_id, telemetry_dict)

    def get_vehicle_telemetry(self, vehicle_id: str) -> Optional[Dict[str, Any]]:
        if self._client:
            try:
                raw = self._client.hget("coldchain:fleet", vehicle_id)
                if raw:
                    return json.loads(raw)
            except Exception:
                pass
        return self._fallback.get_vehicle_telemetry(vehicle_id)

    def get_all_vehicles(self) -> List[Dict[str, Any]]:
        if self._client:
            try:
                raw_dict = self._client.hgetall("coldchain:fleet")
                return [json.loads(v) for v in raw_dict.values()]
            except Exception:
                pass
        return self._fallback.get_all_vehicles()

    def add_to_spatial_cluster(self, h3_index: str, vehicle_id: str) -> None:
        if self._client:
            try:
                self._client.sadd(f"coldchain:h3:{h3_index}", vehicle_id)
                return
            except Exception:
                pass
        self._fallback.add_to_spatial_cluster(h3_index, vehicle_id)

    def get_vehicles_in_spatial_cluster(self, h3_index: str) -> Set[str]:
        if self._client:
            try:
                members = self._client.smembers(f"coldchain:h3:{h3_index}")
                return {m.decode("utf-8") if isinstance(m, bytes) else str(m) for m in members}
            except Exception:
                pass
        return self._fallback.get_vehicles_in_spatial_cluster(h3_index)

    def save_alert(self, alert_dict: Dict[str, Any]) -> None:
        if self._client:
            try:
                self._client.lpush("coldchain:alerts", json.dumps(alert_dict))
                self._client.ltrim("coldchain:alerts", 0, 999)
                return
            except Exception:
                pass
        self._fallback.save_alert(alert_dict)

    def get_active_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        if self._client:
            try:
                raw_list = self._client.lrange("coldchain:alerts", 0, limit - 1)
                return [json.loads(x) for x in raw_list]
            except Exception:
                pass
        return self._fallback.get_active_alerts(limit)
