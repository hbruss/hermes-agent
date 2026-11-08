"""Queued native input keeps its receiving bot across adapter replacement and restart."""
from contextlib import asynccontextmanager

import pytest

from gateway.config import Platform
from gateway.platforms.event import MessageEvent
from gateway.profile_routing import ProfileRoute
from gateway.run_runtime import initialize_gateway_runtime
from gateway.runtime_ownership import process_ownership
from gateway.session_authorities import owner_scope
from gateway.session_contract import SessionRef
from gateway.session_identity import resolve_identity
from gateway.session_ingress_context import native_callback, register_transport_home
from hermes_state_runtime import list_session_admissions
from tests.gateway.test_multiplex_transport_matrix import _stub
from tests.gateway.test_session_authorities_multiplex import _reserve_homes, _runner


@asynccontextmanager
async def _queued(tmp_path, monkeypatch):
    root, homes = _reserve_homes(tmp_path, monkeypatch, names=('alpha',))
    (root / '.env').write_text('TELEGRAM_ALLOWED_USERS=101\n')
    monkeypatch.setenv('TELEGRAM_ALLOWED_USERS', '101')
    process_ownership.reserve([home for _, home in homes])
    runner = _runner(root, homes)
    try:
        runner.config.profile_routes = [ProfileRoute(
            name='shared-bot', platform='telegram', profile='alpha', chat_id='101')]
        await initialize_gateway_runtime(runner)
        authority = runner.session_authorities.for_home(homes[1][1])
        # A LOCAL adapter must not turn the routed runtime into the receiving bot.
        runner._profile_adapters['alpha'] = {Platform.LOCAL: object()}
        adapter = _stub(Platform.TELEGRAM, runner, 'receiving-bot')
        adapter.config.token = 'fixture-connector'
        runner.adapters[Platform.TELEGRAM] = adapter
        register_transport_home(runner, None, root)
        source = adapter.build_source(chat_id='101', chat_type='dm', user_id='101')
        resolve_identity(source, runner=runner)
        event = MessageEvent(text='retained queued input', source=source, message_id='queued-1')
        # Hold the consumer, not admission or route validation: the test claims the real row below.
        monkeypatch.setattr(authority, '_schedule', lambda ref: None)
        with owner_scope(authority), native_callback(runner, event, root):
            await authority._admit_native(event)
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        row, = list_session_admissions(authority.db, session_id=entry.session_id)
        yield runner, authority, row
    finally:
        for authority in list(getattr(runner, 'session_authorities', []) or []):
            authority.db.close()
        for _, home in homes:
            process_ownership.release(home)


@pytest.mark.asyncio
async def test_queued_input_claims_through_reconnected_receiving_bot(tmp_path, monkeypatch):
    async with _queued(tmp_path, monkeypatch) as (runner, authority, queued):
        replacement = _stub(Platform.TELEGRAM, runner, 'reconnected-bot')
        replacement.config.token = 'fixture-connector'
        runner.adapters[Platform.TELEGRAM] = replacement
        sid = queued['target_session_id']
        live = authority.sessions[sid]
        with owner_scope(authority):
            claimed, _ = await authority._claim_next(SessionRef(authority.profile_id, sid), live)
        assert claimed['admission_id'] == queued['admission_id']
        assert claimed['status'] == 'started'
        assert runner._adapter_for_source(live.source) is replacement
        assert claimed['payload'] == queued['payload']


@pytest.mark.asyncio
async def test_startup_recovers_queued_input_using_persisted_receiving_bot(tmp_path, monkeypatch):
    from gateway.run_runtime import recover_profile_native_sessions
    from gateway.session import SessionEntry

    async with _queued(tmp_path, monkeypatch) as (runner, authority, queued):
        sid = queued['target_session_id']
        live = authority.sessions.pop(sid)
        entry = runner.session_store.lookup_by_session_key(live.route)
        # Routing serialization deliberately omits in-process identity/provenance.
        runner.session_store._entries[live.route] = SessionEntry.from_dict(entry.to_dict())
        result = await recover_profile_native_sessions(runner, authority)
        assert result == {sid: 'ready'}
        with owner_scope(authority):
            claimed, _ = await authority._claim_next(
                SessionRef(authority.profile_id, sid), authority.sessions[sid])
        assert claimed['admission_id'] == queued['admission_id']
        assert claimed['status'] == 'started'
        assert claimed['payload'] == queued['payload']
