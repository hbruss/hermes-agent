"""Route-owned admissions and scheduled heartbeats follow an automatic reset atomically."""
import json
import sqlite3

import pytest

from hermes_state import SessionDB
import hermes_state_route_rehome as route_rehome
import hermes_state_runtime as rt
from hermes_cli.heartbeat import HeartbeatState
from hermes_state_keys import admission_fingerprint


ROUTE = 'discord:server:channel'
OLD = 'logical-old'
TIP = 'physical-tip'
NEW = 'new-session'


def _native(route, owner, identity, *, platform='discord', heartbeat=None, media=None):
    automation = {'identity': identity, 'owner': owner}
    if heartbeat is not None:
        automation['heartbeat'] = heartbeat
    native = {
        'source': {'platform': platform, 'chat_id': 'conversation', 'is_bot': False},
        'route': route,
        'timestamp': '1970-01-01T00:00:00+00:00',
        'event': {'message_id': identity},
        'provenance': {'transport_home': 'profile-home', 'runtime_home': 'profile-home',
                       'platform': platform, 'connector': platform},
        'automation': automation,
    }
    if media is not None:
        native['media'] = media
    return {'text': 'automated input', 'native_text_v1': native}


def _admit(db, epoch, *, principal, session_id, request_id, payload):
    return rt.admit_session_input(db, epoch=epoch, principal_id=principal, session_id=session_id,
                                  request_id=request_id, payload=payload)


def _route_webhook(db, epoch, *, delivery, owner=OLD):
    request_id = json.dumps(['webhook', ROUTE, 'alerts', delivery], separators=(',', ':'))
    return _admit(db, epoch, principal='automation:' + ROUTE, session_id=owner, request_id=request_id,
                  payload=_native(ROUTE, owner, request_id))


def _create_conversation(db):
    db.create_session(OLD, source='discord')
    db.create_session(TIP, source='discord', parent_session_id=OLD)
    db.create_session(NEW, source='discord')
    return rt.begin_runtime_epoch(db, instance_id='route-rehome-test')


@pytest.mark.parametrize('status', ['active', 'paused'])
def test_rehome_keeps_ordered_route_automation_and_due_heartbeat_state(tmp_path, status):
    db = SessionDB(db_path=tmp_path / 'state.db')
    try:
        epoch = _create_conversation(db)
        historical = _admit(db, epoch, principal='history', session_id=OLD, request_id='last-native',
                            payload={'text': 'committed source', 'native_text_v1': {
                                'source': {'platform': 'discord', 'chat_id': 'conversation', 'is_bot': False},
                                'route': ROUTE, 'timestamp': '1970-01-01T00:00:00+00:00',
                                'event': {'message_id': 'last-native'},
                                'provenance': {'transport_home': 'profile-home', 'runtime_home': 'profile-home',
                                               'platform': 'discord', 'connector': 'discord'},
                                'media': [{'path': '/staged', 'sha256': 'abc'}]}})
        webhook1 = _route_webhook(db, epoch, delivery='evt-1')
        message = _admit(db, epoch, principal='messaging:discord', session_id=OLD, request_id='human-1',
                         payload={'text': 'ordinary queued input'})
        completion_id = json.dumps(['completion', 'process-1', ''], separators=(',', ':'))
        completion = _admit(db, epoch, principal='automation:' + ROUTE, session_id=OLD,
                            request_id=completion_id,
                            payload=_native(ROUTE, OLD, completion_id))
        process_tick_id = json.dumps(['heartbeat', TIP, 'process-event'], separators=(',', ':'))
        process_tick = _admit(db, epoch, principal='automation:' + ROUTE, session_id=OLD,
                              request_id=process_tick_id,
                              payload=_native(ROUTE, OLD, process_tick_id))
        webhook2 = _route_webhook(db, epoch, delivery='evt-2')
        heartbeat_id = json.dumps(['heartbeat', TIP, 1000.0, 4], separators=(',', ':'))
        heartbeat = _admit(db, epoch, principal='automation:' + ROUTE, session_id=OLD,
                            request_id=heartbeat_id,
                            payload=_native(ROUTE, OLD, heartbeat_id, heartbeat=TIP))
        initial = {row['admission_id']: row for row in rt.list_session_admissions(
            db, session_id=OLD, pending_only=False)}
        scheduled = HeartbeatState(prompt='check status', interval_seconds=300, status=status,
                                   created_at=1000.0, last_fired_at=1125.0, fire_count=4)
        db.set_meta('heartbeat:' + TIP, scheduled.to_json())

        moved = rt.rehome_queued_inputs(
            db, epoch=epoch, old_session_id=OLD, new_session_id=NEW, principal_prefix='messaging:',
            route_key=ROUTE, heartbeat_session_id=TIP)

        expected_moved = [webhook1['admission_id'], message['admission_id'], webhook2['admission_id'],
                          heartbeat['admission_id']]
        assert moved == expected_moved
        with db._read_ctx() as conn:
            after = {row['admission_id']: dict(row) for row in conn.execute(
                'SELECT admission_id,seq,request_id,target_session_id,lineage_json,payload_json,payload_digest '
                'FROM session_admissions')}
        for admission_id in expected_moved:
            before, result = initial[admission_id], after[admission_id]
            assert result['target_session_id'] == NEW
            assert result['request_id'] == before['request_id']
            assert result['seq'] == before['seq']
            assert json.loads(result['lineage_json']) == [NEW]
            payload = json.loads(result['payload_json'])
            if admission_id != message['admission_id']:
                automation = payload['native_text_v1']['automation']
                assert automation['owner'] == NEW
                if admission_id == heartbeat['admission_id']:
                    assert automation['heartbeat'] == NEW
                else:
                    assert 'heartbeat' not in automation
                expected_digest = admission_fingerprint(
                    canonical_target=NEW, payload={'input': payload, 'intent': 'queue'})
                assert result['payload_digest'] == expected_digest
        assert after[completion['admission_id']]['target_session_id'] == OLD
        assert after[historical['admission_id']]['target_session_id'] == OLD
        assert after[process_tick['admission_id']]['target_session_id'] == OLD
        assert moved == [
            item for _sequence, item in sorted((after[item]['seq'], item) for item in expected_moved)
        ]

        new_state = HeartbeatState.from_json(db.get_meta('heartbeat:' + NEW))
        old_state = HeartbeatState.from_json(db.get_meta('heartbeat:' + TIP))
        assert (new_state.status, new_state.created_at, new_state.last_fired_at, new_state.fire_count) == (
            status, scheduled.created_at, scheduled.last_fired_at, scheduled.fire_count)
        assert (old_state.status, old_state.created_at, old_state.last_fired_at, old_state.fire_count) == (
            'cleared', scheduled.created_at, scheduled.last_fired_at, scheduled.fire_count)
        provenance = route_rehome.load_route_provenance(db, NEW, ROUTE)
        assert provenance['route'] == ROUTE
        assert provenance['provenance']['platform'] == 'discord'
        assert 'media' not in provenance
        followup = 'next-automatic-handoff'
        db.create_session(followup, source='discord')
        moved_again = rt.rehome_queued_inputs(
            db, epoch=epoch, old_session_id=NEW, new_session_id=followup, principal_prefix='messaging:',
            route_key=ROUTE, heartbeat_session_id=NEW)
        assert moved_again == expected_moved
        carried = rt.get_session_admission(db, admission_id=heartbeat['admission_id'])
        assert carried['request_id'] == heartbeat_id
        assert carried['payload']['native_text_v1']['automation']['heartbeat'] == followup
        assert HeartbeatState.from_json(db.get_meta('heartbeat:' + followup)).to_json() == scheduled.to_json()
    finally:
        db.close()


def test_route_provenance_marker_follows_resets_without_current_native_admissions(tmp_path):
    db = SessionDB(db_path=tmp_path / 'state.db')
    try:
        epoch = _create_conversation(db)
        followup = 'followup-session'
        db.create_session(followup, source='discord')
        envelope = {
            'source': {'platform': 'discord', 'chat_id': 'conversation', 'is_bot': False},
            'route': ROUTE, 'timestamp': '1970-01-01T00:00:00+00:00',
            'event': {'message_id': 'last-native'},
            'provenance': {'transport_home': 'profile-home', 'runtime_home': 'profile-home',
                           'platform': 'discord', 'connector': 'discord'},
        }
        _admit(db, epoch, principal='history', session_id=OLD, request_id='last-native',
               payload={'text': 'committed source', 'native_text_v1': envelope})

        assert rt.rehome_queued_inputs(
            db, epoch=epoch, old_session_id=OLD, new_session_id=NEW, principal_prefix='messaging:',
            route_key=ROUTE, heartbeat_session_id=TIP) == []
        assert route_rehome.load_route_provenance(db, NEW, ROUTE) == envelope
        assert rt.rehome_queued_inputs(
            db, epoch=epoch, old_session_id=NEW, new_session_id=followup, principal_prefix='messaging:',
            route_key=ROUTE, heartbeat_session_id=NEW) == []
        assert route_rehome.load_route_provenance(db, followup, ROUTE) == envelope
        from hermes_state_mutation_retirement import retire_routes
        db._execute_write(lambda conn: retire_routes(conn, [followup]))
        assert route_rehome.load_route_provenance(db, followup, ROUTE) is None
        assert route_rehome.load_route_provenance(db, NEW, ROUTE) == envelope
    finally:
        db.close()


def test_route_rehome_rolls_back_earlier_admission_and_metadata_writes(tmp_path):
    db = SessionDB(db_path=tmp_path / 'state.db')
    try:
        epoch = _create_conversation(db)
        _admit(db, epoch, principal='history', session_id=OLD, request_id='last-native',
               payload={'text': 'committed source', 'native_text_v1': {
                   'source': {'platform': 'discord', 'chat_id': 'conversation', 'is_bot': False},
                   'route': ROUTE, 'timestamp': '1970-01-01T00:00:00+00:00',
                   'event': {'message_id': 'last-native'},
                   'provenance': {'transport_home': 'profile-home', 'runtime_home': 'profile-home',
                                  'platform': 'discord', 'connector': 'discord'}}})
        first = _route_webhook(db, epoch, delivery='evt-1')
        second = _route_webhook(db, epoch, delivery='evt-2')
        scheduled = HeartbeatState(prompt='check status', interval_seconds=300, status='paused',
                                   created_at=1000.0, last_fired_at=1125.0, fire_count=4)
        db.set_meta('heartbeat:' + TIP, scheduled.to_json())
        before = {row['admission_id']: row for row in rt.list_session_admissions(
            db, session_id=OLD, pending_only=False)}
        db._execute_write(lambda conn: conn.execute(
            f"CREATE TRIGGER refuse_second_route_move BEFORE UPDATE OF target_session_id ON session_admissions "
            f"WHEN OLD.admission_id='{second['admission_id']}' BEGIN "
            "SELECT RAISE(ABORT, 'refused second route admission'); END"))

        with pytest.raises(sqlite3.IntegrityError, match='refused second route admission'):
            rt.rehome_queued_inputs(
                db, epoch=epoch, old_session_id=OLD, new_session_id=NEW, principal_prefix='messaging:',
                route_key=ROUTE, heartbeat_session_id=TIP)

        restored = {row['admission_id']: row for row in rt.list_session_admissions(
            db, session_id=OLD, pending_only=False)}
        assert set(restored) == set(before)
        assert restored[first['admission_id']]['target_session_id'] == OLD
        assert restored[second['admission_id']]['target_session_id'] == OLD
        assert db.get_meta('heartbeat:' + TIP) == scheduled.to_json()
        assert db.get_meta('heartbeat:' + NEW) is None
        assert route_rehome.load_route_provenance(db, NEW, ROUTE) is None
        assert rt.list_session_admissions(db, session_id=NEW, pending_only=False) == []
    finally:
        db.close()


def test_queued_heartbeat_survives_compression_before_handoff(tmp_path):
    db = SessionDB(db_path=tmp_path / 'state.db')
    try:
        db.create_session(OLD, source='discord')
        db.create_session(NEW, source='discord')
        epoch = rt.begin_runtime_epoch(db, instance_id='compressed-heartbeat')
        identity = json.dumps(['heartbeat', OLD, 1000.0, 1], separators=(',', ':'))
        queued = _admit(db, epoch, principal='automation:' + ROUTE, session_id=OLD, request_id=identity,
                        payload=_native(ROUTE, OLD, identity, heartbeat=OLD))
        db.publish_compression_child(
            parent_session_id=OLD, child_session_id=TIP, source='discord',
            require_compression_lease=False, model='offline', model_config={},
            system_prompt='offline', messages=[{'role': 'user', 'content': 'retained'}])
        scheduled = HeartbeatState(prompt='check status', interval_seconds=300,
                                   created_at=1000.0, last_fired_at=1125.0, fire_count=1)
        db.set_meta('heartbeat:' + TIP, scheduled.to_json())
        assert db.get_compression_tip(OLD) == TIP

        moved = rt.rehome_queued_inputs(
            db, epoch=epoch, old_session_id=OLD, new_session_id=NEW, principal_prefix='messaging:',
            route_key=ROUTE, heartbeat_session_id=TIP)

        assert moved == [queued['admission_id']]
        carried = rt.get_session_admission(db, admission_id=queued['admission_id'])
        assert carried['request_id'] == identity
        assert carried['target_session_id'] == NEW
        assert carried['payload']['native_text_v1']['automation']['heartbeat'] == NEW
        assert rt.list_session_admissions(db, session_id=OLD) == []
        assert HeartbeatState.from_json(db.get_meta('heartbeat:' + NEW)).to_json() == scheduled.to_json()
    finally:
        db.close()


def test_malformed_owned_identity_refuses_the_whole_handoff(tmp_path):
    db = SessionDB(db_path=tmp_path / 'state.db')
    try:
        epoch = _create_conversation(db)
        identity = json.dumps(['webhook', ROUTE, 'alerts', 'owned-event'], separators=(',', ':'))
        queued = _admit(db, epoch, principal='automation:' + ROUTE, session_id=OLD,
                        request_id='malformed-identity', payload=_native(ROUTE, OLD, identity))
        with pytest.raises(rt.RuntimeStoreError, match='admission_conflict'):
            rt.rehome_queued_inputs(
                db, epoch=epoch, old_session_id=OLD, new_session_id=NEW, principal_prefix='messaging:',
                route_key=ROUTE, heartbeat_session_id=TIP)
        assert rt.get_session_admission(db, admission_id=queued['admission_id'])['target_session_id'] == OLD
        assert route_rehome.load_route_provenance(db, NEW, ROUTE) is None
    finally:
        db.close()


@pytest.mark.parametrize('departed', [None, 'cleared'])
def test_existing_successor_schedule_refuses_handoff_without_old_schedule(tmp_path, departed):
    db = SessionDB(db_path=tmp_path / 'state.db')
    try:
        epoch = _create_conversation(db)
        target = HeartbeatState(prompt='already owned', interval_seconds=300, created_at=1000.0)
        db.set_meta('heartbeat:' + NEW, target.to_json())
        if departed == 'cleared':
            db.set_meta('heartbeat:' + TIP, HeartbeatState(
                prompt='departed', interval_seconds=300, status='cleared', created_at=900.0).to_json())
        with pytest.raises(rt.RuntimeStoreError, match='admission_conflict'):
            rt.rehome_queued_inputs(
                db, epoch=epoch, old_session_id=OLD, new_session_id=NEW, principal_prefix='messaging:',
                route_key=ROUTE, heartbeat_session_id=TIP)
        assert db.get_meta('heartbeat:' + NEW) == target.to_json()
    finally:
        db.close()


def test_native_empty_cleared_heartbeat_does_not_block_handoff(tmp_path):
    db = SessionDB(db_path=tmp_path / 'state.db')
    try:
        epoch = _create_conversation(db)
        db.set_meta('heartbeat:' + TIP, '')
        assert rt.rehome_queued_inputs(
            db, epoch=epoch, old_session_id=OLD, new_session_id=NEW, principal_prefix='messaging:',
            route_key=ROUTE, heartbeat_session_id=TIP) == []
        assert db.get_meta('heartbeat:' + NEW) is None
    finally:
        db.close()
