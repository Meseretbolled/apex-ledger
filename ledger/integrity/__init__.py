from .audit_chain import AuditChain, ChainVerificationResult, compute_event_hash


async def run_integrity_check(event_store, stream_id: str):
    from ledger.events import IntegrityCheckResult

    chain = AuditChain(event_store)
    verification = await chain.verify_stream(stream_id)

    result = IntegrityCheckResult(
        chain_valid=verification.is_valid,
        tamper_detected=not verification.is_valid,
        checked_events=verification.events_checked,
    )

    current_version = await event_store.stream_version(stream_id)
    await event_store.append(
        stream_id=stream_id,
        expected_version=current_version,
        events=[
            {
                "event_type": "AuditIntegrityCheckRun",
                "event_version": 1,
                "payload": result.model_dump(),
                "metadata": {"triggering_event_id": "system-integrity-check"},
            }
        ],
    )

    return result


__all__ = [
    "AuditChain",
    "ChainVerificationResult",
    "compute_event_hash",
    "run_integrity_check",
]