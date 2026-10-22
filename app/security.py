"""HMAC verification for callbacks; always sign the unmodified request bytes."""

import hashlib
import hmac

from fastapi import HTTPException, status


def verify_webhook_signature(body: bytes, signature: str | None, secret: str) -> None:
    """Reject missing or invalid hex-encoded HMAC-SHA256 signatures."""

    if signature is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid webhook signature",
        )

    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid webhook signature",
        )
