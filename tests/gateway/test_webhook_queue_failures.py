"""Queue routes fail closed without falling through to ordinary webhook sessions."""
import asyncio
from contextlib import asynccontextmanager
import hashlib
import hmac
import json

import aiohttp
import pytest


@asynccontextmanager
async def _running_queue(monkeypatch, *, foreign_profile_target=False):
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent
    from gateway.platforms.webhook import WebhookAdapter
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from gateway.session_results import execution_result
    from plugins.platforms.discord.adapter import DiscordAdapter

    monkeypatch.setenv("DISCORD_ALLOWED_USERS", "42")
    runner = GatewayRunner(GatewayConfig())
    discord = DiscordAdapter(PlatformConfig(enabled=True, token="fixture-token", typing_indicator=False))
    discord.gateway_runner = runner
    runner.adapters = {Platform.DISCORD: discord}
    authority = await initialize_session_authority(runner, profile_id="default", instance_id="queue-failures")
    runner._wire_adapter_handlers(discord)
    source = discord.build_source(chat_id="42", chat_type="dm", user_id="42")
    target_key = runner.session_store._generate_session_key(source)
    started, finish = asyncio.Event(), asyncio.Event()
    sent = []

    async def send(chat_id, content, reply_to=None, metadata=None):
        sent.append((chat_id, content))
        return SendResult(success=True, message_id="sent")

    async def turn(event, source, quick_key, run_generation):
        if event.text == "human work":
            started.set()
            await finish.wait()
        result = execution_result.get()
        if result is not None:
            result["result"] = {"final_response": "handled " + event.text, "messages": []}
        return "handled " + event.text

    monkeypatch.setattr(discord, "send", send)
    monkeypatch.setattr(runner, "_handle_message_with_agent", turn)
    await discord.handle_message(MessageEvent(text="human work", source=source, message_id="human"))
    await asyncio.wait_for(started.wait(), 10)
    owner = runner.session_store.peek_session_id(target_key)

    if foreign_profile_target:
        foreign_source = discord.build_source(chat_id="43", chat_type="dm", user_id="43")
        foreign_source.profile = "secondary"
        with monkeypatch.context() as scoped:
            scoped.setattr(runner.config, "multiplex_profiles", True)
            scoped.setattr(runner.session_store.config, "multiplex_profiles", True)
            target_key = runner.session_store._generate_session_key(foreign_source)
        from hermes_state_keys import profile_from_session_key
        assert profile_from_session_key(target_key) == "secondary"

    webhook = WebhookAdapter(PlatformConfig(enabled=True, extra={
        "host": "127.0.0.1", "port": 0, "secret": "queue-fixture-secret",
        "routes": {"fixture": {"prompt": "{text}", "queue_to_session": target_key}},
    }))
    webhook.gateway_runner = runner
    runner.adapters[Platform.WEBHOOK] = webhook
    runner._wire_adapter_handlers(webhook)
    assert await webhook.connect()
    port = next(iter(webhook._runner.sites))._server.sockets[0].getsockname()[1]
    url = f"http://127.0.0.1:{port}/webhooks/fixture"
    try:
        yield runner, authority, webhook, owner, url, finish, sent
    finally:
        finish.set()
        await webhook.disconnect()


def _signed(body, *, delivery_id="failure-delivery"):
    return {
        "X-GitHub-Delivery": delivery_id,
        "X-Hub-Signature-256": "sha256=" + hmac.new(
            b"queue-fixture-secret", body, hashlib.sha256).hexdigest(),
    }


@pytest.mark.asyncio
async def test_unavailable_native_admission_returns_503_without_webhook_fallthrough(monkeypatch):
    import sqlite3

    async with _running_queue(monkeypatch) as (_runner, authority, _webhook, owner, url, _finish, _sent):
        def unavailable(*_args, **_kwargs):
            raise sqlite3.OperationalError("database is locked")

        with monkeypatch.context() as fault:
            fault.setattr(authority.db, "_execute_write", unavailable)
            body = json.dumps({"text": "alert evidence"}).encode()
            async with aiohttp.ClientSession() as client:
                response = await client.post(url, data=body, headers=_signed(body))
                assert response.status == 503, await response.text()
        rows = authority.db._read_all("SELECT target_session_id, principal_id FROM session_admissions")
        assert len(rows) == 1 and rows[0]["target_session_id"] == owner
        assert not rows[0]["principal_id"].startswith("automation:")


@pytest.mark.asyncio
async def test_foreign_profile_session_key_is_refused_without_creating_a_webhook_session(monkeypatch):
    async with _running_queue(monkeypatch, foreign_profile_target=True) as (
            _runner, authority, _webhook, owner, url, _finish, _sent):
        body = json.dumps({"text": "alert evidence"}).encode()
        async with aiohttp.ClientSession() as client:
            response = await client.post(url, data=body, headers=_signed(body))
            assert response.status == 503, await response.text()
        rows = authority.db._read_all("SELECT target_session_id, principal_id FROM session_admissions")
        assert len(rows) == 1 and rows[0]["target_session_id"] == owner
        assert not rows[0]["principal_id"].startswith("automation:")


@pytest.mark.asyncio
async def test_queue_route_rejects_missing_provider_delivery_identity(monkeypatch):
    async with _running_queue(monkeypatch) as (_runner, authority, _webhook, owner, url, _finish, _sent):
        body = json.dumps({"text": "alert evidence"}).encode()
        headers = _signed(body)
        headers.pop("X-GitHub-Delivery")
        async with aiohttp.ClientSession() as client:
            response = await client.post(url, data=body, headers=headers)
            assert response.status == 400, await response.text()
        rows = authority.db._read_all("SELECT target_session_id FROM session_admissions")
        assert len(rows) == 1 and rows[0]["target_session_id"] == owner

@pytest.mark.parametrize(
    ("option", "value"),
    [("script", "ignore.py"), ("coalesce", {}), ("cron_job", "daily"), ("deliver_only", True),
     ("mcp_event", {})],
)
def test_queue_route_rejects_incompatible_acknowledgement_paths(option, value):
    from gateway.platforms.webhook_queue import validate_queue_route

    route = {"queue_to_session": "agent:main:discord:dm:42", option: value}
    with pytest.raises(ValueError, match="cannot be combined"):
        validate_queue_route("fixture", route)


@pytest.mark.asyncio
async def test_dynamic_subscription_cannot_queue_into_an_existing_conversation(monkeypatch):
    from hermes_constants import get_hermes_home

    async with _running_queue(monkeypatch) as (_runner, authority, webhook, owner, url, _finish, _sent):
        route = dict(webhook._routes["fixture"], secret="queue-fixture-secret")
        (get_hermes_home() / "webhook_subscriptions.json").write_text(json.dumps({"dynamic": route}))
        body = json.dumps({"text": "unapproved route evidence"}).encode()
        async with aiohttp.ClientSession() as client:
            response = await client.post(url.rsplit("/", 1)[0] + "/dynamic", data=body, headers=_signed(body))
            assert response.status == 404, await response.text()
        rows = authority.db._read_all("SELECT target_session_id FROM session_admissions")
        assert len(rows) == 1 and rows[0]["target_session_id"] == owner


@pytest.mark.asyncio
@pytest.mark.parametrize("secret_scope", ["route", "global"])
async def test_queue_routes_require_signatures_even_on_loopback(secret_scope):
    from gateway.config import PlatformConfig
    from gateway.platforms.webhook import WebhookAdapter, _INSECURE_NO_AUTH

    route = {"queue_to_session": "agent:main:discord:dm:42", "prompt": "{text}"}
    extra = {"host": "127.0.0.1", "port": 0, "secret": "fixture-secret", "routes": {"fixture": route}}
    if secret_scope == "route":
        route["secret"] = _INSECURE_NO_AUTH
    else:
        extra["secret"] = _INSECURE_NO_AUTH
    webhook = WebhookAdapter(PlatformConfig(enabled=True, extra=extra))
    try:
        with pytest.raises(ValueError, match="queue_to_session.*authenticated"):
            await webhook.connect()
    finally:
        await webhook.disconnect()
