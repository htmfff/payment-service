"""Minimal webhook endpoint for local demos.

Receives `payment.succeeded` / `payment.failed` callbacks, verifies the HMAC signature and
prints a short summary. Started by `docker compose --profile demo up webhook-demo`.
"""

from __future__ import annotations

import argparse
import hmac
import json
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from app.config import get_settings
from app.logging import configure_logging, get_logger

logger = get_logger("webhook-demo")
_SIGNATURE_HEADER = "X-Signature"
_MAX_BODY_BYTES = 1_048_576


class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "PaymentWebhookDemo/1.0"
    signing_secret = ""

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        length = int(self.headers.get("Content-Length", "0"))
        if length > _MAX_BODY_BYTES:
            self._respond(413, {"error": "payload too large"})
            return

        body = self.rfile.read(length)
        event = self.headers.get("X-Payment-Event", "unknown")

        if not self._signature_matches(body):
            logger.warning("webhook signature mismatch", extra={"event": event, "path": self.path})
            self._respond(401, {"error": "invalid signature"})
            return

        payload = self._parse(body)
        logger.info(
            "webhook received",
            extra={
                "event": event,
                "payment_id": payload.get("payment_id"),
                "status": payload.get("status"),
                "amount": payload.get("amount"),
                "currency": payload.get("currency"),
            },
        )
        self._respond(200, {"status": "accepted"})

    def log_message(self, format: str, *args: Any) -> None:
        logger.debug("http access", extra={"detail": format % args})

    def _signature_matches(self, body: bytes) -> bool:
        provided = self.headers.get(_SIGNATURE_HEADER, "")
        expected = hmac.new(
            self.signing_secret.encode("utf-8"),
            body,
            sha256,
        ).hexdigest()
        return hmac.compare_digest(provided.removeprefix("sha256="), expected)

    @staticmethod
    def _parse(body: bytes) -> dict[str, Any]:
        try:
            decoded = json.loads(body or b"{}")
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}

    def _respond(self, status_code: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    parser = argparse.ArgumentParser(description="Demo webhook receiver")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    arguments = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)
    WebhookHandler.signing_secret = settings.webhook_signing_secret.get_secret_value()

    server = ThreadingHTTPServer((arguments.host, arguments.port), WebhookHandler)
    logger.info(
        "webhook demo receiver listening",
        extra={"host": arguments.host, "port": arguments.port},
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()