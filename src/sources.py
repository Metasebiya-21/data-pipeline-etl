import os
from typing import Dict, List

# "mongo_db=label,mongo_db=label": which MongoDB databases to load, and the label stored
# in Snowflake as source_database.
DEFAULT_SOURCES = "coop-customer-management-db=michu_coop,wegagen-customer-management-db=efoyta_wegagen"


def parse_sources(spec: str) -> List[Dict[str, str]]:
    sources = []
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        mongo_db, _, label = item.partition("=")
        sources.append({"mongo_db": mongo_db.strip(), "label": (label or mongo_db).strip()})
    return sources


def configured_sources() -> List[Dict[str, str]]:
    spec = os.environ.get("MONGO_SOURCES") or DEFAULT_SOURCES
    return parse_sources(spec)
