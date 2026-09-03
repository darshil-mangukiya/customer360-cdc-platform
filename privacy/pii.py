from __future__ import annotations

import hashlib
import os
import re
from typing import Any


DEFAULT_SALT = "customer-360-local-demo-salt"
PII_FIELD_CLASSES = {
    "email": "direct_identifier",
    "primary_email": "direct_identifier",
    "phone": "direct_identifier",
    "primary_phone": "direct_identifier",
    "first_name": "quasi_identifier",
    "last_name": "quasi_identifier",
    "external_account_id": "pseudonymous_identifier",
    "device_id": "pseudonymous_identifier",
}


def privacy_salt() -> str:
    return os.getenv("PII_HASH_SALT", DEFAULT_SALT)


def normalize_email(value: Any) -> str | None:
    if not value:
        return None
    email = str(value).strip().lower()
    if "@" not in email:
        return None
    local, domain = email.split("@", 1)
    if "+" in local:
        local = local.split("+", 1)[0]
    return f"{local}@{domain}"


def normalize_phone(value: Any) -> str | None:
    if not value:
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) < 10:
        return None
    if len(digits) == 10:
        digits = f"1{digits}"
    return f"+{digits}"


def hash_identifier(value: Any, *, salt: str | None = None) -> str | None:
    if value in {None, ""}:
        return None
    digest_input = f"{salt or privacy_salt()}::{str(value).strip().lower()}"
    return hashlib.sha256(digest_input.encode("utf-8")).hexdigest()


def hash_email(value: Any, *, salt: str | None = None) -> str | None:
    normalized = normalize_email(value)
    return hash_identifier(normalized, salt=salt) if normalized else None


def hash_phone(value: Any, *, salt: str | None = None) -> str | None:
    normalized = normalize_phone(value)
    return hash_identifier(normalized, salt=salt) if normalized else None


def mask_email(value: Any) -> str | None:
    normalized = normalize_email(value)
    if not normalized:
        return None
    local, domain = normalized.split("@", 1)
    if len(local) <= 2:
        masked_local = f"{local[0]}***"
    else:
        masked_local = f"{local[0]}***{local[-1]}"
    return f"{masked_local}@{domain}"


def mask_phone(value: Any) -> str | None:
    normalized = normalize_phone(value)
    if not normalized:
        return None
    return f"{normalized[:3]}***{normalized[-4:]}"


def classify_field(field_name: str) -> str:
    return PII_FIELD_CLASSES.get(field_name, "non_pii")

