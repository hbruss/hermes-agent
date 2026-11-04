"""Queue routes fail closed without falling through to ordinary webhook sessions."""
import asyncio
from contextlib import asynccontextmanager
import hashlib
import hmac
import json

import aiohttp
import pytest


@asynccontextmanager
async def _running_queue(monkeypatch, *, foreign_profile_target=False, unserved_ingress=False):
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent
    from gateway.platforms.webhook import WebhookAdapter
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from gateway.session_results import execution_result
    from plugins.platforms.discord.adapter import DiscordAdapter

    monkeypatch.setenv("DISCORD_ALLOWED_USERS", "42")
    config = GatewayConfig()
    if unserved_ingress:
        from gateway.profile_routing import ProfileRoute
        from hermes_constants import get_hermes_home
        config.multiplex_profiles = True
        config._runtime_profile_homes = (("default", get_hermes_home()),)
        config.profile_routes = [
            ProfileRoute(name="unserved", platform="webhook", profile="unserved", user_id="webhook:fixture")]
        monkeypatch.setattr("hermes_cli.profiles.profiles_to_serve",
                            lambda **kwargs: [("default", get_hermes_home())])
    runner = GatewayRunner(config)
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


@pytest.mark.asyncio
async def test_rejected_ingress_profile_cannot_borrow_the_launch_conversation(monkeypatch):
    async with _running_queue(monkeypatch, unserved_ingress=True) as (
            _runner, authority, _webhook, owner, url, _finish, _sent):
        body = json.dumps({"text": "must not run in launch profile"}).encode()
        async with aiohttp.ClientSession() as client:
            response = await client.post(url, data=body, headers=_signed(body))
            assert response.status == 503, await response.text()
        rows = authority.db._read_all("SELECT target_session_id FROM session_admissions")
        assert len(rows) == 1 and rows[0]["target_session_id"] == owner


@pytest.mark.asyncio
async def test_failed_handoff_restores_route_and_preserves_accepted_automation(monkeypatch):
    import sqlite3
    from gateway.session_authority import keyed_turn
    from gateway.session_automation import reset_and_rehome_messages
    from hermes_cli.heartbeat import HeartbeatState
    from hermes_state_runtime import list_session_admissions
    from tests.waiting import async_wait_until

    async with _running_queue(monkeypatch) as (runner, authority, webhook, owner, url, finish, _sent):
        finish.set()
        await async_wait_until(
            lambda: all(row["status"] == "terminal" for row in list_session_admissions(
                authority.db, session_id=owner, pending_only=False)), timeout=10)
        key = webhook._routes["fixture"]["queue_to_session"]
        runner.session_store.set_model_override(key, {"model": "fixture-model"})
        runner.session_store.set_session_yolo(key, False)
        before = runner.session_store.lookup_by_session_key(key).to_dict()
        gate = authority.sessions[owner].claim_gate
        await gate.acquire()
        try:
            heartbeat = HeartbeatState(prompt="owned wake", interval_seconds=60, created_at=1000).to_json()
            authority.db.set_meta("heartbeat:" + owner, heartbeat)
            body = b'{"text":"durably accepted event"}'
            async with aiohttp.ClientSession() as client:
                response = await client.post(url, data=body, headers=_signed(body))
                assert response.status == 202, await response.text()
                receipt = (await response.json())["receipt"]["admission_id"]
            authority.db._execute_write(lambda conn: conn.execute(
                "CREATE TRIGGER fail_owned_rehome BEFORE UPDATE OF target_session_id ON session_admissions "
                "WHEN OLD.target_session_id != NEW.target_session_id "
                "BEGIN SELECT RAISE(ABORT, 'injected rehome write failure'); END"))
            try:
                async with keyed_turn(authority._route_turns, key):
                    with pytest.raises(sqlite3.IntegrityError, match="injected rehome"):
                        await reset_and_rehome_messages(runner, key, owner)
            finally:
                authority.db._execute_write(lambda conn: conn.execute("DROP TRIGGER fail_owned_rehome"))
            assert runner.session_store.lookup_by_session_key(key).to_dict() == before
            persisted = runner.session_store._routing_db._read_one(
                "SELECT entry_json FROM gateway_routing WHERE scope=? AND session_key=?",
                (runner.session_store._routing_scope(), key))
            assert json.loads(persisted["entry_json"]) == before
            assert authority.db.get_session(owner)["ended_at"] is None
            pending = list_session_admissions(authority.db, session_id=owner)
            assert [row["admission_id"] for row in pending] == [receipt]
            assert authority.db.get_meta("heartbeat:" + owner) == heartbeat
            assert owner not in authority.rotated_owners
            async with keyed_turn(authority._route_turns, key):
                successor, release = await reset_and_rehome_messages(runner, key, owner)
            try:
                assert [row["admission_id"] for row in list_session_admissions(
                    authority.db, session_id=successor.session_id)] == [receipt]
                assert json.loads(authority.db.get_meta("heartbeat:" + successor.session_id)) == json.loads(heartbeat)
            finally:
                release()
        finally:
            gate.release()
