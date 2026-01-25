import json
from datetime import datetime
from typing import Any, Dict

from bson import ObjectId


def normalize_mongo_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {}
    for key, value in doc.items():
        normalized[key] = _serialize_value(value)
    if "_id" in normalized:
        normalized["_id"] = str(normalized["_id"])
    return normalized


def _serialize_value(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_serialize_value(v) for v in value]
    return value


def to_json_payload(doc: Dict[str, Any]) -> str:
    return json.dumps(doc, separators=(",", ":"), ensure_ascii=True)
