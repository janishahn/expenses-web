import time

from itsdangerous import BadSignature, URLSafeSerializer

from config import get_settings


def _serializer() -> URLSafeSerializer:
    settings = get_settings()
    return URLSafeSerializer(settings.csrf_secret, salt="csrf-token")


def generate_csrf_token(user_id: int = 1, max_age_hours: int = 2) -> str:
    """
    Generate a CSRF token with expiration time.

    Args:
        user_id: User ID (defaults to 1 for single-user app)
        max_age_hours: Token validity period in hours (default: 2 hours)
    """
    serializer = _serializer()
    timestamp = int(time.time())
    expiry = timestamp + (max_age_hours * 3600)

    token_data = {"u": user_id, "ts": timestamp, "exp": expiry}

    return serializer.dumps(token_data)


def validate_csrf_token(token: str, user_id: int = 1, max_age_hours: int = 2) -> bool:
    """
    Validate a CSRF token with expiration check.

    Args:
        token: The CSRF token to validate
        user_id: User ID (defaults to 1 for single-user app)
        max_age_hours: Expected token validity period in hours (default: 2 hours)
    """
    serializer = _serializer()
    try:
        data = serializer.loads(token, max_age=max_age_hours * 3600)
    except BadSignature:
        return False

    # Check user ID matches
    if data.get("u") != user_id:
        return False

    # Additional check for explicit expiration
    current_time = int(time.time())
    expiry_time = data.get("exp", 0)

    if current_time > expiry_time:
        return False

    return True


def get_token_age_seconds(token: str) -> int:
    """
    Get the age of a CSRF token in seconds for debugging/monitoring.
    Returns -1 if token is invalid.
    """
    serializer = _serializer()
    try:
        data = serializer.loads(
            token, max_age=24 * 3600
        )  # Allow up to 24h for age check
        current_time = int(time.time())
        token_timestamp = data.get("ts", current_time)
        return current_time - token_timestamp
    except BadSignature:
        return -1
