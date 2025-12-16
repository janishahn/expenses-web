import time

from itsdangerous import BadSignature, URLSafeSerializer

from config import get_settings


def _serializer() -> URLSafeSerializer:
    settings = get_settings()
    return URLSafeSerializer(settings.csrf_secret, salt="csrf-token")


def generate_csrf_token(user_id: int = 1, max_age_hours: int = 2) -> str:
    serializer = _serializer()
    timestamp = int(time.time())
    expiry = timestamp + (max_age_hours * 3600)

    token_data = {"u": user_id, "ts": timestamp, "exp": expiry}

    return serializer.dumps(token_data)


def validate_csrf_token(token: str, user_id: int = 1, max_age_hours: int = 2) -> bool:
    serializer = _serializer()
    try:
        data = serializer.loads(token, max_age=max_age_hours * 3600)
    except BadSignature:
        return False

    if data.get("u") != user_id:
        return False

    current_time = int(time.time())
    expiry_time = data.get("exp", 0)

    if current_time > expiry_time:
        return False

    return True
