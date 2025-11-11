from __future__ import annotations

from itsdangerous import BadSignature, URLSafeSerializer

from config import get_settings


def _serializer() -> URLSafeSerializer:
    settings = get_settings()
    return URLSafeSerializer(settings.csrf_secret, salt="csrf-token")


def generate_csrf_token(user_id: int = 1) -> str:
    serializer = _serializer()
    return serializer.dumps({"u": user_id})


def validate_csrf_token(token: str, user_id: int = 1) -> bool:
    serializer = _serializer()
    try:
        data = serializer.loads(token)
    except BadSignature:
        return False
    return data.get("u") == user_id
