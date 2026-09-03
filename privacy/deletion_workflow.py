from __future__ import annotations

import argparse
import csv
import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from privacy.pii import hash_email


@dataclass(frozen=True)
class DeletionRequest:
    deletion_request_id: str
    tenant_id: str
    canonical_customer_id: str
    email_sha256: str
    request_type: str
    requested_at: str
    status: str
    handling_notes: str


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def build_deletion_request(
    *,
    tenant_id: str,
    canonical_customer_id: str,
    email: str,
    request_type: str = "gdpr_erasure",
) -> DeletionRequest:
    requested_at = _now()
    digest = hashlib.sha256(
        f"{tenant_id}:{canonical_customer_id}:{email}:{requested_at}".encode("utf-8")
    ).hexdigest()[:16]
    return DeletionRequest(
        deletion_request_id=f"del_{digest}",
        tenant_id=tenant_id,
        canonical_customer_id=canonical_customer_id,
        email_sha256=hash_email(email) or "",
        request_type=request_type,
        requested_at=requested_at,
        status="queued_for_privacy_review",
        handling_notes="Suppress activation exports immediately; retain raw CDC only under legal/audit retention policy.",
    )


def write_deletion_outputs(request: DeletionRequest, output_dir: Path) -> None:
    """Append this deletion request to `deletion_requests.csv`.

    Previously this also overwrote `activation_suppression_list.csv` with just this
    one row, which would (a) clobber any consent-based suppressions already computed
    by `reverse_etl.exporter` / `privacy.activation_policy`, and (b) discard every
    earlier deletion request on each new call. `activation_suppression_list.csv` is
    now written only by `privacy.activation_policy.write_privacy_activation_outputs`,
    which is the single place that has the full canonical-customer + consent context
    needed to compute it correctly; deletion requests feed into that computation via
    `reverse_etl.exporter.main`'s `--deletion-requests-path` (loaded from the file
    this function appends to).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "deletion_requests.csv"
    fieldnames = list(asdict(request).keys())
    file_exists = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(asdict(request))


def load_deletion_requests(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def main() -> None:
    parser = argparse.ArgumentParser(description="Create privacy deletion and activation suppression artifacts.")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--canonical-customer-id", required=True)
    parser.add_argument("--email", required=True)
    parser.add_argument("--request-type", default="gdpr_erasure")
    parser.add_argument("--output-dir", default="privacy/output")
    args = parser.parse_args()
    request = build_deletion_request(
        tenant_id=args.tenant_id,
        canonical_customer_id=args.canonical_customer_id,
        email=args.email,
        request_type=args.request_type,
    )
    write_deletion_outputs(request, Path(args.output_dir))
    print(f"deletion_request_id={request.deletion_request_id} output_dir={args.output_dir}")


if __name__ == "__main__":
    main()
