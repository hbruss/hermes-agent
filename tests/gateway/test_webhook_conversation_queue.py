"""Authenticated webhook acceptance belongs to the selected conversation's native FIFO."""
import asyncio
import hashlib
import hmac
import json

import aiohttp
import pytest

from tests.waiting import async_wait_until


@pytest.mark.asyncio
async def test_signed_delivery_queues_behind_the_existing_conversation(monkeypatch):
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent
    from gateway.platforms.webhook import WebhookAdapter
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from hermes_state_runtime import list_session_admissions
    from plugins.platforms.discord.adapter import DiscordAdapter

    monkeypatch.setenv('DISCORD_ALLOWED_USERS', '42')
    runner = GatewayRunner(GatewayConfig())
    discord = DiscordAdapter(PlatformConfig(enabled=True, token='fixture-token', typing_indicator=False))
    discord.gateway_runner = runner
    runner.adapters = {Platform.DISCORD: discord}
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='queue-test')
    runner._wire_adapter_handlers(discord)
    source = discord.build_source(chat_id='42', chat_type='dm', user_id='42')
    key = runner.session_store._generate_session_key(source)
    started, finish, delivered = asyncio.Event(), asyncio.Event(), asyncio.Event()
    executed, sent = [], []

    async def send(chat_id, content, reply_to=None, metadata=None):
        sent.append((chat_id, content))
        if chat_id == '42' and content == 'queued event handled':
            delivered.set()
        return SendResult(success=True, message_id='sent')

    async def model(message, history, session_id, **kwargs):
        executed.append((message, session_id))
        if message == 'human work':
            started.set()
            await finish.wait()
        response = 'queued event handled' if 'alert evidence' in message else 'human work completed'
        return {'final_response': response, 'messages': [
            *history, {'role': 'user', 'content': message}, {'role': 'assistant', 'content': response}],
            'api_calls': 1, 'tools': [], 'history_offset': len(history), 'session_id': session_id}

    monkeypatch.setattr(discord, 'send', send)
    monkeypatch.setattr(runner, '_run_agent', model)
    await discord.handle_message(MessageEvent(text='human work', source=source, message_id='human'))
    await asyncio.wait_for(started.wait(), 10)
    owner = runner.session_store.peek_session_id(key)
    webhook = WebhookAdapter(PlatformConfig(enabled=True, extra={
        'host': '127.0.0.1', 'port': 0, 'secret': 'queue-fixture-secret',
        'routes': {'fixture': {'prompt': '{text}', 'queue_to_session': key}},
    }))
    webhook.gateway_runner = runner
    runner.adapters[Platform.WEBHOOK] = webhook
    runner._wire_adapter_handlers(webhook)
    assert await webhook.connect()
    port = next(iter(webhook._runner.sites))._server.sockets[0].getsockname()[1]
    url = f'http://127.0.0.1:{port}/webhooks/fixture'

    def signed(text):
        body = json.dumps({'text': text}).encode()
        return body, {
            'X-GitHub-Delivery': 'delivery-1',
            'X-Hub-Signature-256': 'sha256=' + hmac.new(
                b'queue-fixture-secret', body, hashlib.sha256).hexdigest(),
        }

    try:
        async with aiohttp.ClientSession() as client:
            body, headers = signed('alert evidence')
            response = await client.post(url, data=body, headers=headers)
            assert response.status == 202, await response.text()
            rows = list_session_admissions(authority.db, session_id=owner, pending_only=False)
            assert len(rows) == 2, 'the alert must commit to the busy Discord owner, not a webhook session'
            alert = rows[-1]
            assert alert['status'] == 'queued'
            assert alert['principal_id'] == 'automation:' + key
            assert executed == [('human work', owner)]
            duplicate = await client.post(url, data=body, headers=headers)
            assert duplicate.status == 200, await duplicate.text()
            changed, changed_headers = signed('different evidence')
            conflict = await client.post(url, data=changed, headers=changed_headers)
            assert conflict.status == 409, await conflict.text()
            assert len(list_session_admissions(authority.db, session_id=owner, pending_only=False)) == 2
            finish.set()
            await async_wait_until(
                lambda: all(r['status'] == 'terminal' for r in list_session_admissions(
                    authority.db, session_id=owner, pending_only=False)), timeout=15,
                what='both inputs to finish on the single owner')
            assert [sid for _, sid in executed] == [owner, owner]
            assert 'alert evidence' in executed[-1][0]
            await asyncio.wait_for(delivered.wait(), 10)
            assert ('42', 'queued event handled') in sent
    finally:
        finish.set()
        await webhook.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize('queue_heartbeat', [True, False])
async def test_automatic_handoff_preserves_events_wake_and_retry_identity(monkeypatch, queue_heartbeat):
    import time
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent
    from gateway.platforms.webhook import WebhookAdapter
    from gateway.run import GatewayRunner
    from gateway.run_heartbeat_acceptance import admit_heartbeat
    from gateway.session_authority import initialize_session_authority, keyed_turn
    from gateway.session_automation import committed_route_envelope, reset_and_rehome_messages
    from hermes_cli.heartbeat import HeartbeatManager, HeartbeatState, save_heartbeat
    from hermes_state_runtime import list_session_admissions
    from plugins.platforms.discord.adapter import DiscordAdapter
    import gateway.session_webhook_queue as queue_ingress

    monkeypatch.setenv('DISCORD_ALLOWED_USERS', '42')
    runner = GatewayRunner(GatewayConfig())
    discord = DiscordAdapter(PlatformConfig(enabled=True, token='fixture-token', typing_indicator=False))
    discord.gateway_runner = runner
    runner.adapters = {Platform.DISCORD: discord}
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='handoff-test')
    runner._wire_adapter_handlers(discord)
    source = discord.build_source(chat_id='42', chat_type='dm', user_id='42')
    key = runner.session_store._generate_session_key(source)
    executed = []
    seed_started = asyncio.Event()
    queue_entered = asyncio.Event()
    admit_delivery = queue_ingress.admit_webhook_delivery

    async def observed_admission(authority, **kwargs):
        if kwargs['external_delivery_id'] == 'after-handoff-event':
            queue_entered.set()
        return await admit_delivery(authority, **kwargs)

    monkeypatch.setattr(queue_ingress, 'admit_webhook_delivery', observed_admission)

    async def send(chat_id, content, reply_to=None, metadata=None):
        return SendResult(success=True, message_id='sent')

    async def model(message, history, session_id, **kwargs):
        executed.append((message, session_id))
        if message == 'seed':
            seed_started.set()
        return {'final_response': 'handled', 'messages': [
            *history, {'role': 'user', 'content': message}, {'role': 'assistant', 'content': 'handled'}],
            'api_calls': 1, 'tools': [], 'history_offset': len(history), 'session_id': session_id}

    monkeypatch.setattr(discord, 'send', send)
    monkeypatch.setattr(runner, '_run_agent', model)
    await discord.handle_message(MessageEvent(text='seed', source=source, message_id='seed'))
    await asyncio.wait_for(seed_started.wait(), 10)
    old = runner.session_store.peek_session_id(key)
    await async_wait_until(
        lambda: all(r['status'] == 'terminal' for r in list_session_admissions(
            authority.db, session_id=old, pending_only=False)), timeout=15,
        what='the seed admission to settle')
    old_gate = authority.sessions[old].claim_gate
    await old_gate.acquire()
    save_heartbeat(old, HeartbeatState(prompt='owned scheduled work', interval_seconds=60,
                                       created_at=time.time() - 61))
    if queue_heartbeat:
        await admit_heartbeat(runner, discord, source, old, key)
    heartbeat_before = HeartbeatManager(old).state.to_json()
    webhook = WebhookAdapter(PlatformConfig(enabled=True, extra={
        'host': '127.0.0.1', 'port': 0, 'secret': 'handoff-fixture-secret',
        'routes': {'fixture': {'prompt': '{text}', 'queue_to_session': key}},
    }))
    webhook.gateway_runner = runner
    runner.adapters[Platform.WEBHOOK] = webhook
    runner._wire_adapter_handlers(webhook)
    assert await webhook.connect()
    port = next(iter(webhook._runner.sites))._server.sockets[0].getsockname()[1]
    url = f'http://127.0.0.1:{port}/webhooks/fixture'
    body = b'{"text":"owned event"}'
    headers = {
        'X-GitHub-Delivery': 'handoff-event',
        'X-Hub-Signature-256': 'sha256=' + hmac.new(
            b'handoff-fixture-secret', body, hashlib.sha256).hexdigest(),
    }
    release = None
    late_request = None
    try:
        async with aiohttp.ClientSession() as client:
            accepted = await client.post(url, data=body, headers=headers)
            assert accepted.status == 202, await accepted.text()
            before = list_session_admissions(authority.db, session_id=old)
            assert len(before) == (2 if queue_heartbeat else 1)
            assert all(r['status'] == 'queued' for r in before)
            async with keyed_turn(authority._route_turns, key):
                late_request = asyncio.create_task(client.post(url, data=body, headers={
                    **headers, 'X-GitHub-Delivery': 'after-handoff-event'}))
                await asyncio.wait_for(queue_entered.wait(), 10)
                fresh, release = await reset_and_rehome_messages(runner, key, old)
                new = fresh.session_id
                old_gate.release()
                after = list_session_admissions(authority.db, session_id=new)
            assert [(r['admission_id'], r['request_id'], r['seq']) for r in after] == [
                (r['admission_id'], r['request_id'], r['seq']) for r in before]
            assert all(r['payload']['native_text_v1']['automation']['owner'] == new for r in after)
            if queue_heartbeat:
                assert after[0]['payload']['native_text_v1']['automation']['heartbeat'] == new
            assert HeartbeatManager(new).state.to_json() == heartbeat_before
            assert HeartbeatManager(old).state is None
            assert not list_session_admissions(authority.db, session_id=old)
            assert executed == [('seed', old)], 'the successor must stay behind its recenter gate'
            duplicate = await client.post(url, data=body, headers=headers)
            assert duplicate.status == 200, await duplicate.text()
            fresh_event = await late_request
            assert fresh_event.status == 202, await fresh_event.text()
            assert len(list_session_admissions(authority.db, session_id=new)) == (3 if queue_heartbeat else 2)
            release()
            release = None
            if not queue_heartbeat:
                await admit_heartbeat(runner, discord, source, new, key)
            await async_wait_until(
                lambda: all(r['status'] == 'terminal' for r in list_session_admissions(
                    authority.db, session_id=new, pending_only=False)), timeout=15,
                what='the carried wake and webhook events to finish')
            assert [sid for _, sid in executed[1:]] == [new, new, new]
            expected = (['owned scheduled work', 'owned event', 'owned event'] if queue_heartbeat
                        else ['owned event', 'owned event', 'owned scheduled work'])
            assert all(marker in text for marker, (text, _) in zip(expected, executed[1:]))
            terminal_retry = await client.post(url, data=body, headers=headers)
            assert terminal_retry.status == 200, await terminal_retry.text()
            assert len(list_session_admissions(authority.db, session_id=new, pending_only=False)) == 3
            # The store's ordinary reset path must not inherit automatic-handoff provenance.
            manual = runner.session_store.reset_session(key)
            assert manual.session_id != new
            assert committed_route_envelope(authority, manual) is None
            assert len(executed) == 4
    finally:
        if old_gate.locked():
            old_gate.release()
        if release is not None:
            release()
        if late_request is not None:
            if not late_request.done():
                late_request.cancel()
            await asyncio.gather(late_request, return_exceptions=True)
        await webhook.disconnect()
