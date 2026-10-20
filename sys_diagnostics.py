"""Live diagnostics for notification APIs, MT5 relay auth, and strategy config.

This script intentionally executes no strategy or trading logic. Live checks
post mock notifications to configured destinations and to the operations
relay, so running it can create visible test messages in Telegram/Slack.

Usage:
    python sys_diagnostics.py
    python sys_diagnostics.py --env /path/to/.env --params config/strategy_params.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import aiohttp


MOCK_TEXT = "[SYSTEM DIAGNOSTICS] Mock connectivity test. No strategy or order was run."


def read_env_file(path: Path) -> tuple[dict[str, str], str | None]:
    """Read a small dotenv-compatible KEY=VALUE file without logging secrets."""
    if not path.exists():
        return {}, f".env file not found at {path}"
    if not path.is_file():
        return {}, f".env path is not a file: {path}"
    values: dict[str, str] = {}
    try:
        for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].lstrip()
            if "=" not in line:
                return {}, f"Malformed .env entry on line {line_number}; expected KEY=VALUE"
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if not key or not key.replace("_", "").isalnum() or key[0].isdigit():
                return {}, f"Invalid environment variable name on line {line_number}"
            # Remove matching quotes. For unquoted values, a whitespace-prefixed
            # comment is ignored; literal '#' inside tokens remains intact.
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            else:
                comment = value.find(" #")
                if comment >= 0:
                    value = value[:comment].rstrip()
            values[key] = value
    except OSError as exc:
        return {}, f"Could not read .env file: {type(exc).__name__}"
    return values, None


def resolve(name: str, dotenv: dict[str, str]) -> str | None:
    """Environment variables override values loaded from the selected .env."""
    value = os.environ.get(name, dotenv.get(name, ""))
    return value.strip() or None


async def test_telegram(session: aiohttp.ClientSession,
                        env: dict[str, str]) -> tuple[bool, str]:
    token = resolve("TELEGRAM_BOT_TOKEN", env)
    chat_id = resolve("TELEGRAM_CHAT_ID", env)
    missing = []
    if not token:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not chat_id:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        return False, "Missing " + " and ".join(missing) + " in .env or process environment"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        async with session.post(url, json={"chat_id": chat_id, "text": MOCK_TEXT}) as response:
            body = await _response_json(response)
            if response.status < 200 or response.status >= 300:
                description = _telegram_description(body)
                return False, (f"Telegram sendMessage returned HTTP {response.status}: {description}. "
                               "Check TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, and bot access to the chat.")
            if not isinstance(body, dict) or body.get("ok") is not True:
                description = _telegram_description(body)
                return False, (f"Telegram rejected sendMessage: {description}. Check that "
                               "TELEGRAM_BOT_TOKEN is valid and the bot can message TELEGRAM_CHAT_ID.")
            return True, "Telegram Bot API accepted the mock notification"
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        # Do not include exception strings: they can contain the bot token URL.
        return False, f"Could not reach Telegram Bot API ({type(exc).__name__}); check outbound HTTPS/DNS"


async def test_slack(session: aiohttp.ClientSession,
                     env: dict[str, str]) -> tuple[bool, str]:
    token = resolve("SLACK_BOT_TOKEN", env)
    channel = resolve("SLACK_CHANNEL_ID", env)
    missing = []
    if not token:
        missing.append("SLACK_BOT_TOKEN")
    if not channel:
        missing.append("SLACK_CHANNEL_ID")
    if missing:
        return False, "Missing " + " and ".join(missing) + " in .env or process environment"

    try:
        async with session.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}"},
            json={"channel": channel, "text": MOCK_TEXT, "mrkdwn": False,
                  "unfurl_links": False, "unfurl_media": False},
        ) as response:
            body = await _response_json(response)
            if response.status < 200 or response.status >= 300:
                return False, (f"Slack chat.postMessage returned HTTP {response.status}. "
                               "Check SLACK_BOT_TOKEN, channel ID, and outbound HTTPS.")
            if not isinstance(body, dict) or body.get("ok") is not True:
                error = body.get("error", "invalid response") if isinstance(body, dict) else "invalid response"
                guidance = {
                    "invalid_auth": "SLACK_BOT_TOKEN is invalid or revoked",
                    "missing_scope": "Slack app is missing the chat:write scope",
                    "channel_not_found": "SLACK_CHANNEL_ID is invalid or inaccessible",
                    "not_in_channel": "Invite the Slack bot to the configured channel",
                    "not_authed": "SLACK_BOT_TOKEN was not accepted",
                }.get(str(error), "check the Slack token, channel ID, app scopes, and channel membership")
                return False, f"Slack rejected chat.postMessage ({error}): {guidance}"
            return True, "Slack API accepted the mock notification"
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        return False, f"Could not reach Slack API ({type(exc).__name__}); check outbound HTTPS/DNS"


async def test_relay(session: aiohttp.ClientSession,
                     env: dict[str, str]) -> tuple[bool, str]:
    relay_url = resolve("OPS_RELAY_URL", env)
    ingest_token = resolve("OPS_INGEST_TOKEN", env)
    missing = []
    if not relay_url:
        missing.append("OPS_RELAY_URL")
    if not ingest_token:
        missing.append("OPS_INGEST_TOKEN")
    if missing:
        return False, "Missing " + " and ".join(missing) + " in .env or process environment"
    if len(ingest_token) < 24:
        return False, "OPS_INGEST_TOKEN is shorter than 24 characters; configure the same strong token on the relay"

    endpoint = relay_url.rstrip("/")
    if not endpoint.endswith("/events"):
        endpoint += "/events"
    payload = {
        "event_type": "error",
        "payload": {
            "component": "sys_diagnostics",
            "message": "Mock authenticated relay connectivity test; no strategy or order was run.",
        },
    }
    try:
        async with session.post(
            endpoint,
            headers={"Authorization": f"Bearer {ingest_token}"},
            json=payload,
        ) as response:
            body = await _response_json(response)
            if response.status == 401:
                return False, ("Linux relay returned 401 Unauthorized; OPS_INGEST_TOKEN does not "
                               "match the relay's configured token")
            if response.status == 404:
                return False, (f"Relay endpoint not found at {endpoint}; check OPS_RELAY_URL and "
                               "that the Linux relay exposes POST /events")
            if response.status < 200 or response.status >= 300:
                detail = body.get("error", body.get("message", "")) if isinstance(body, dict) else ""
                return False, (f"Relay returned HTTP {response.status} {detail}; check OPS_RELAY_URL, "
                               "OPS_INGEST_TOKEN, and relay logs")
            if not isinstance(body, dict) or body.get("queued") is not True:
                return False, "Relay responded but did not confirm the test event was queued"
            return True, "Authenticated relay accepted the mock error event (202 queued)"
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        return False, (f"Could not connect to Linux relay ({type(exc).__name__}); check OPS_RELAY_URL, "
                       "routing/firewall, relay health, and TLS configuration")


def test_strategy_params(path: Path) -> tuple[bool, str, dict[str, Any] | None]:
    if not path.exists():
        return False, f"Strategy parameter file not found: {path}", None
    if not path.is_file():
        return False, f"Strategy parameter path is not a file: {path}", None
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return False, f"Invalid JSON in {path} at line {exc.lineno}, column {exc.colno}: {exc.msg}", None
    except OSError as exc:
        return False, f"Could not read {path}: {type(exc).__name__}", None
    if not isinstance(parsed, dict):
        return False, f"Expected a JSON object in {path}, got {type(parsed).__name__}", None
    return True, f"Parsed strategy parameters from {path}", parsed


async def run(env_path: Path, params_path: Path, timeout: float) -> int:
    env, env_error = read_env_file(env_path)
    if env_error:
        print(f"[FAIL] .env: {env_error}")
    else:
        print(f"[ OK ] Loaded .env variables from {env_path} (secret values suppressed)")

    timeout_cfg = aiohttp.ClientTimeout(total=timeout)
    connector = aiohttp.TCPConnector(ssl=True)
    async with aiohttp.ClientSession(timeout=timeout_cfg, connector=connector) as session:
        telegram_ok, telegram_msg = await test_telegram(session, env)
        _print_result("Telegram Bot API", telegram_ok, telegram_msg)
        slack_ok, slack_msg = await test_slack(session, env)
        _print_result("Slack API", slack_ok, slack_msg)
        relay_ok, relay_msg = await test_relay(session, env)
        _print_result("Authenticated MT5-to-Linux relay", relay_ok, relay_msg)

    params_ok, params_msg, params = test_strategy_params(params_path)
    _print_result("Strategy parameters", params_ok, params_msg)
    if params_ok:
        print("Parsed strategy_params dictionary:")
        print(json.dumps(params, indent=2, ensure_ascii=False, sort_keys=True))

    failures = int(not telegram_ok) + int(not slack_ok) + int(not relay_ok) + int(not params_ok)
    print(f"\nDiagnostics complete: {4 - failures}/4 checks passed, {failures} failed.")
    return 0 if failures == 0 else 1


def _print_result(name: str, passed: bool, message: str) -> None:
    print(f"[{ ' OK ' if passed else 'FAIL'}] {name}: {message}")


async def _response_json(response: aiohttp.ClientResponse) -> Any:
    try:
        return await response.json(content_type=None)
    except (aiohttp.ContentTypeError, json.JSONDecodeError, ValueError):
        return {"description": (await response.text())[:300]}


def _telegram_description(body: Any) -> str:
    if isinstance(body, dict):
        return str(body.get("description", body.get("error_code", "unknown API response")))[:300]
    return "invalid API response"


def main() -> None:
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=root / ".env",
                        help="dotenv file (default: .env next to this script)")
    parser.add_argument("--params", type=Path, default=root / "config" / "strategy_params.json",
                        help="strategy JSON (default: config/strategy_params.json)")
    parser.add_argument("--timeout", type=float, default=8.0,
                        help="per-request timeout seconds (default: 8)")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    raise SystemExit(asyncio.run(run(args.env, args.params, args.timeout)))


if __name__ == "__main__":
    main()
