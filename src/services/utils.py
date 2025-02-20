from typing import Any, Dict, List
import json
from bson import ObjectId

def flatten_document(document: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """Achata documentos MongoDB com estruturas aninhadas"""
    items: List[tuple] = []
    for k, v in document.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_document(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, (dict, list)):
                    items.extend(flatten_document(item, f"{new_key}[{i}]", sep=sep).items())
                else:
                    items.append((f"{new_key}[{i}]", item))
        else:
            items.append((new_key, v))
    return dict(items)

def serialize_document(document: Dict[str, Any]) -> str:
    """
    Converte documentos do MongoDB
    """
    return json.dumps(document, default=str)