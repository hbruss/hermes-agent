"""Durable native-session admission for opt-in authenticated webhook routes."""
import json

from hermes_state_runtime import RuntimeStoreError, get_session_admission


def delivery_request_id(target_route_key: str, route_name: str, external_delivery_id: str) -> str:
    """Stable, route-scoped native identity for one provider delivery."""
    return json.dumps(["webhook", target_route_key, route_name, external_delivery_id],
                      separators=(",", ":"))


async def admit_webhook_delivery(authority, *, target_route_key: str, route_name: str,
                                 external_delivery_id: str, text: str):
    """Resolve and admit under the same route cutover lock used by native handoffs.

    Returns ``(receipt, replayed)``. The receipt is always the durable authority receipt; a retry
    never starts a second execution.
    """
    from gateway.session_authority import keyed_turn

    request_id = delivery_request_id(target_route_key, route_name, external_delivery_id)
    principal = "automation:" + target_route_key
    async with keyed_turn(authority._route_turns, target_route_key):
        runner = authority.runner
        entry = runner.session_store.lookup_by_session_key(target_route_key)
        if entry is None or entry.suspended:
            raise RuntimeStoreError("not_found")

        from gateway.session_automation import committed_route_envelope, snapshot_automation
        committed = committed_route_envelope(authority, entry)
        if committed is None:
            raise RuntimeStoreError("not_found")
        from gateway.session_envelope import restore_native
        envelope = {key: value for key, value in committed.items() if key != "media"}
        restored = restore_native({"text": text, "native_text_v1": envelope}, runner)
        source = restored.source
        if runner.session_store._generate_session_key(source) != target_route_key:
            raise RuntimeStoreError("admission_conflict")

        from gateway.session_authorities import authority_for_home
        target_home = runner._resolve_profile_home_for_source(source)
        if authority_for_home(runner, target_home) is not authority:
            raise RuntimeStoreError("profile_mismatch")

        from gateway.config import Platform
        if source.platform in {Platform.API_SERVER, Platform.LOCAL, Platform.WEBHOOK}:
            raise RuntimeStoreError("not_found")
        target_adapter = runner._adapter_for_source(source)
        if target_adapter is None:
            raise RuntimeStoreError("not_found")

        from gateway.platforms.event import MessageEvent, MessageType
        event = MessageEvent(text=text, message_type=MessageType.TEXT, source=source, internal=True,
                             message_id=request_id,
                             metadata={"gateway_session_key": target_route_key,
                                       "gateway_session_id": entry.session_id})
        candidate, resolved_entry = snapshot_automation(authority, target_adapter, event, request_id)
        if resolved_entry.session_key != target_route_key:
            raise RuntimeStoreError("admission_conflict")

        prior = _prior_receipts(authority, principal, request_id)
        if prior:
            if len(prior) != 1 or not _same_delivery(prior[0], candidate, target_route_key):
                raise RuntimeStoreError("admission_conflict")
            return authority._receipt(prior[0]), True

        receipt = await authority.admit_automation(target_adapter, event, request_id)
        if receipt is None:
            raise RuntimeStoreError("admission_unavailable")
        return receipt, False


def _prior_receipts(authority, principal: str, request_id: str) -> list[dict]:
    """Find live and retired native receipts by their stable route identity."""
    from hermes_state_terminal import ADMISSION_PREFIX

    with authority.db._read_ctx() as conn:
        admission_ids = [row[0] for row in conn.execute(
            "SELECT admission_id FROM session_admissions WHERE principal_id=? AND request_id=?",
            (principal, request_id))]
        retired = conn.execute(
            "SELECT value FROM state_meta WHERE key GLOB ? AND json_valid(value) "
            "AND json_extract(value, '$.principal_id')=? "
            "AND json_extract(value, '$.request_id')=?",
            (ADMISSION_PREFIX + "*", principal, request_id)).fetchall()
        admission_ids.extend(json.loads(row[0])["admission_id"] for row in retired)
    return [get_session_admission(authority.db, admission_id=admission_id) for admission_id in admission_ids]


def _same_delivery(row: dict, candidate: dict, target_route_key: str) -> bool:
    """Match retained immutable evidence; deleted content cannot authorize a new execution."""
    payload = row.get("payload") or {}
    if payload:
        envelope = payload.get("native_text_v1")
        automation = envelope.get("automation") if isinstance(envelope, dict) else None
        return (payload.get("text") == candidate.get("text")
                and isinstance(envelope, dict) and envelope.get("route") == target_route_key
                and isinstance(automation, dict) and automation.get("identity") == row["request_id"]
                and automation.get("owner") == row["target_session_id"])

    return False
