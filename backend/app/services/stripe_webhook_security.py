from __future__ import annotations

import hashlib
import hmac
import time
from typing import Any

from app.core.config import settings


class StripeSignatureError(ValueError):
    pass


def _configured_secret(secret: str | None = None) -> str:
    return (secret if secret is not None else settings.stripe_webhook_secret or "").strip()


def _parse_signature_header(signature_header: str | None) -> dict[str, list[str]]:
    parts: dict[str, list[str]] = {}
    for item in (signature_header or "").split(","):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key and value:
            parts.setdefault(key, []).append(value)
    return parts


def build_stripe_signature_header(
    raw_body: bytes,
    *,
    secret: str | None = None,
    timestamp: int | None = None,
) -> str:
    configured_secret = _configured_secret(secret)
    if not configured_secret:
        raise StripeSignatureError("stripe webhook secret is not configured")
    ts = int(timestamp if timestamp is not None else time.time())
    signed_payload = f"{ts}.{raw_body.decode('utf-8')}".encode("utf-8")
    digest = hmac.new(configured_secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    return f"t={ts},v1={digest}"


def verify_stripe_signature_header(
    raw_body: bytes,
    signature_header: str | None,
    *,
    secret: str | None = None,
    tolerance_seconds: int = 300,
    now: int | None = None,
) -> dict[str, Any]:
    configured_secret = _configured_secret(secret)
    if not configured_secret:
        return {"status": "skipped", "reason": "stripe webhook secret is not configured"}
    if not signature_header:
        raise StripeSignatureError("missing Stripe-Signature header")

    parsed = _parse_signature_header(signature_header)
    timestamps = parsed.get("t") or []
    signatures = parsed.get("v1") or []
    if not timestamps or not signatures:
        raise StripeSignatureError("Stripe-Signature header is missing t or v1")

    try:
        timestamp = int(timestamps[-1])
    except Exception as exc:
        raise StripeSignatureError("Stripe-Signature timestamp is invalid") from exc

    current = int(now if now is not None else time.time())
    if tolerance_seconds > 0 and abs(current - timestamp) > tolerance_seconds:
        raise StripeSignatureError("Stripe-Signature timestamp is outside tolerance")

    signed_payload = f"{timestamp}.{raw_body.decode('utf-8')}".encode("utf-8")
    expected = hmac.new(configured_secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    if not any(hmac.compare_digest(expected, candidate) for candidate in signatures):
        raise StripeSignatureError("Stripe-Signature v1 does not match payload")

    return {
        "status": "ok",
        "timestamp": timestamp,
        "tolerance_seconds": tolerance_seconds,
        "signature_count": len(signatures),
    }
