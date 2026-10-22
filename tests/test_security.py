import hashlib
import hmac

import pytest
from fastapi import HTTPException

from app.security import verify_webhook_signature


def test_hmac_uses_raw_bytes() -> None:
    body = b'{"payment_id":"123", "tokens":1}'
    secret = "some-shared-secret"
    signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    verify_webhook_signature(body, signature, secret)

    with pytest.raises(HTTPException) as err:
        verify_webhook_signature(body.replace(b" ", b""), signature, secret)
    assert err.value.status_code == 401


def test_missing_signature_is_rejected() -> None:
    with pytest.raises(HTTPException) as err:
        verify_webhook_signature(b"{}", None, "secret")
    assert err.value.status_code == 401
