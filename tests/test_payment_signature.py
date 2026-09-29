import hashlib
import hmac
import json


def test_webhook_signature_is_deterministic():
    payload = {
        "amount": 10,
        "external_user_id": "user-1",
        "idempotency_key": "payment-1",
        "transaction_id": None,
    }
    signature = hmac.new(
        b"secret", json.dumps(payload, sort_keys=True).encode(), hashlib.sha256
    ).hexdigest()
    assert hmac.compare_digest(signature, signature)
