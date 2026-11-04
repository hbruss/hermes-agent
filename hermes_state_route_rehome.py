"""Transaction-local handoff for route-owned automation and native route proof."""

import json
import math
from datetime import datetime

from hermes_state_keys import admission_fingerprint
from hermes_state_runtime import _text

_ROUTE_PROVENANCE_PREFIX = 'gateway.route_provenance.v1.'


def _error(reason='admission_conflict'):
    from hermes_state_runtime import RuntimeStoreError
    return RuntimeStoreError(reason)


def _route_key(session_id):
    return _ROUTE_PROVENANCE_PREFIX + session_id


def _identity(request_id):
    try:
        value = json.loads(request_id)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, list) else None


def _native(payload):
    native = payload.get('native_text_v1') if isinstance(payload, dict) else None
    return native if isinstance(native, dict) else None


def _candidate_kind(row, payload):
    native = _native(payload) or {}
    automation = native.get('automation')
    automation = automation if isinstance(automation, dict) else {}
    event = native.get('event')
    event = event if isinstance(event, dict) else {}
    raw_ids = (row['request_id'], automation.get('identity'), event.get('message_id'))
    identities = [_identity(value) for value in raw_ids]
    webhook = (any(identity and identity[0] == 'webhook' for identity in identities)
               or any(isinstance(value, str) and value.startswith('["webhook",') for value in raw_ids))
    # Three-part heartbeat IDs belong to process liveness, not the session schedule.
    heartbeat = ('heartbeat' in automation or any(
        identity and identity[0] == 'heartbeat' and len(identity) != 3 for identity in identities))
    if webhook and heartbeat:
        raise _error()
    return 'webhook' if webhook else 'heartbeat' if heartbeat else None


def _validate_automation(db, conn, row, payload, kind, *, old_session_id, route_key, heartbeat_session_id):
    if (row['intent'] != 'queue' or not isinstance(payload, dict) or set(payload) != {'text', 'native_text_v1'}
            or not isinstance(payload['text'], str)):
        raise _error()
    native = payload['native_text_v1']
    if not isinstance(native, dict):
        raise _error()
    automation = native.get('automation')
    source = native.get('source')
    provenance = native.get('provenance')
    event = native.get('event')
    if (native.get('route') != route_key or not isinstance(source, dict)
            or not isinstance(source.get('platform'), str) or not isinstance(automation, dict)
            or not isinstance(provenance, dict) or not provenance or not isinstance(event, dict)
            or automation.get('owner') != old_session_id or automation.get('identity') != row['request_id']
            or event.get('message_id') != row['request_id']):
        raise _error()
    try:
        identity = json.loads(row['request_id'])
        compact = json.dumps(identity, separators=(',', ':'), allow_nan=False)
    except (TypeError, ValueError):
        raise _error() from None
    if compact != row['request_id'] or not isinstance(identity, list):
        raise _error()
    if kind == 'webhook':
        if (len(identity) != 4 or identity[0] != 'webhook' or identity[1] != route_key
                or not all(isinstance(value, str) and value for value in identity[2:])
                or 'heartbeat' in automation):
            raise _error()
    else:
        marker = automation.get('heartbeat')
        if (len(identity) != 4 or identity[0] != 'heartbeat'
                or not isinstance(identity[1], str) or not identity[1]
                or not isinstance(marker, str) or not marker
                or isinstance(identity[2], bool) or not isinstance(identity[2], (int, float))
                or not math.isfinite(identity[2]) or type(identity[3]) is not int or identity[3] < 0):
            raise _error()
        if marker != heartbeat_session_id and db._compression_tip_on(conn, marker) != heartbeat_session_id:
            raise _error()


def _route_envelope(native, route_key):
    if (not isinstance(native, dict) or native.get('route') != route_key
            or not isinstance(native.get('source'), dict)
            or not isinstance(native['source'].get('platform'), str)
            or not isinstance(native.get('provenance'), dict) or not native['provenance']
            or not isinstance(native.get('event'), dict)
            or not isinstance(native.get('timestamp'), str)):
        raise _error()
    try:
        datetime.fromisoformat(native['timestamp'])
    except ValueError:
        raise _error() from None
    return {key: value for key, value in native.items() if key != 'media'}


def _latest_route_envelope(conn, session_id, route_key):
    rows = conn.execute('SELECT payload_json FROM session_admissions WHERE target_session_id=? ORDER BY seq DESC',
                        (session_id,))
    for (encoded,) in rows:
        try:
            payload = json.loads(encoded)
        except (TypeError, ValueError):
            continue
        native = _native(payload)
        if native is not None and native.get('route') == route_key and 'provenance' in native:
            return _route_envelope(native, route_key)
    return None


def _stored_route_envelope(conn, session_id, route_key):
    row = conn.execute('SELECT value FROM state_meta WHERE key=?', (_route_key(session_id),)).fetchone()
    if row is None:
        return None
    try:
        native = json.loads(row['value'])
    except (TypeError, ValueError):
        raise _error() from None
    return _route_envelope(native, route_key)


def _stored_heartbeat(conn, key):
    from hermes_cli.heartbeat import HeartbeatState

    row = conn.execute('SELECT value FROM state_meta WHERE key=?', (key,)).fetchone()
    if row is None or not row[0]:
        return None
    try:
        state = HeartbeatState.from_json(row[0])
    except (AttributeError, TypeError, ValueError, KeyError):
        raise _error() from None
    if state.status == 'cleared':
        return None
    if (state.status not in ('active', 'paused') or not state.prompt or state.interval_seconds <= 0
            or state.fire_count < 0 or not math.isfinite(state.created_at) or not math.isfinite(state.last_fired_at)):
        raise _error()
    return state


def _migrate_heartbeat(conn, new_session_id, heartbeat_session_id):
    from hermes_cli.heartbeat import _META_PREFIX

    old_key, new_key = _META_PREFIX + heartbeat_session_id, _META_PREFIX + new_session_id
    if _stored_heartbeat(conn, new_key) is not None:
        raise _error()
    state = _stored_heartbeat(conn, old_key)
    if state is None:
        return False
    conn.execute('INSERT INTO state_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                 (new_key, state.to_json()))
    state.status = 'cleared'
    conn.execute('UPDATE state_meta SET value=? WHERE key=?', (state.to_json(), old_key))
    return True


def rehome_route_automation(db, conn, *, old_session_id, new_session_id, route_key, heartbeat_session_id):
    """Move queued webhook/session-heartbeat admissions and automation state on the caller's transaction."""
    old_session_id, new_session_id = _text(old_session_id), _text(new_session_id)
    route_key, heartbeat_session_id = _text(route_key), _text(heartbeat_session_id)
    from hermes_state_runtime import _json
    if old_session_id == new_session_id:
        raise _error('invalid_params')

    source_envelope = _latest_route_envelope(conn, old_session_id, route_key)
    if source_envelope is None:
        source_envelope = _stored_route_envelope(conn, old_session_id, route_key)
    if source_envelope is not None:
        existing = _stored_route_envelope(conn, new_session_id, route_key)
        if existing is not None and existing != source_envelope:
            raise _error()

    principal_id = 'automation:' + route_key
    rows = conn.execute("SELECT * FROM session_admissions WHERE target_session_id=? AND principal_id=? "
                        "AND status='queued' ORDER BY seq", (old_session_id, principal_id)).fetchall()
    moves = []
    for row in rows:
        try:
            payload = json.loads(row['payload_json'])
        except (TypeError, ValueError):
            if _candidate_kind(row, {}) is not None:
                raise _error() from None
            continue
        kind = _candidate_kind(row, payload)
        if kind is None:
            continue
        _validate_automation(db, conn, row, payload, kind, old_session_id=old_session_id, route_key=route_key,
                             heartbeat_session_id=heartbeat_session_id)
        native = payload['native_text_v1']
        native['automation']['owner'] = new_session_id
        if kind == 'heartbeat':
            native['automation']['heartbeat'] = new_session_id
        digest = admission_fingerprint(canonical_target=new_session_id,
                                      payload={'input': payload, 'intent': row['intent']})
        moves.append((row, payload, digest))

    _migrate_heartbeat(conn, new_session_id, heartbeat_session_id)
    admission_ids = []
    for row, payload, digest in moves:
        collision = conn.execute('SELECT 1 FROM session_admissions WHERE principal_id=? AND target_session_id=? '
                                'AND request_id=?', (principal_id, new_session_id, row['request_id'])).fetchone()
        if collision is not None:
            raise _error()
        changed = conn.execute('UPDATE session_admissions SET target_session_id=?,payload_json=?,payload_digest=?, '
                               'lineage_json=? WHERE admission_id=? AND target_session_id=? AND status=\'queued\'',
                               (new_session_id, _json(payload),
                                digest, json.dumps([new_session_id]), row['admission_id'], old_session_id)).rowcount
        if changed != 1:
            raise _error()
        admission_ids.append(row['admission_id'])

    if source_envelope is not None:
        conn.execute('INSERT INTO state_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO NOTHING',
                     (_route_key(new_session_id), _json(source_envelope)))
    return admission_ids


def load_route_provenance(db, session_id, route_key):
    """Read the bounded native route proof inherited by an automatic reset, if present."""
    session_id, route_key = _text(session_id), _text(route_key)
    with db._read_ctx() as conn:
        return _stored_route_envelope(conn, session_id, route_key)
