from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Tuple


class UpcasterRegistry:
    """
    Central registry for event upcasting.

    Supports two calling styles:

    1. Old style:
        upcast(event_dict) -> event_dict

    2. New style:
        upcast(event_type, event_version, payload, metadata)
        -> (payload, new_version, metadata)
    """

    def upcast(self, *args, **kwargs):
        # Old style: upcast(event_dict)
        if len(args) == 1 and isinstance(args[0], dict):
            original_event = args[0]
            event = deepcopy(original_event)

            event_type = event.get("event_type")
            event_version = event.get("event_version", 1)
            payload = deepcopy(event.get("payload", {}))
            metadata_present = "metadata" in event
            metadata = deepcopy(event.get("metadata", {}))

            payload, new_version, metadata = self._upcast_parts(
                event_type, event_version, payload, metadata
            )

            # If nothing changed, return exact shape unchanged
            if (
                new_version == event_version
                and payload == event.get("payload", {})
                and (
                    (metadata_present and metadata == event.get("metadata", {}))
                    or (not metadata_present and metadata == {})
                )
            ):
                return deepcopy(original_event)

            event["payload"] = payload
            event["event_version"] = new_version

            if metadata_present or metadata:
                event["metadata"] = metadata

            return event

        # New style: upcast(event_type, event_version, payload, metadata)
        if len(args) == 4:
            event_type, event_version, payload, metadata = args
            return self._upcast_parts(
                event_type,
                event_version,
                deepcopy(payload or {}),
                deepcopy(metadata or {}),
            )

        raise TypeError(
            "UpcasterRegistry.upcast() expects either "
            "(event_dict) or (event_type, event_version, payload, metadata)"
        )

    def _upcast_parts(
        self,
        event_type: str,
        event_version: int,
        payload: Dict[str, Any],
        metadata: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], int, Dict[str, Any]]:
        payload = dict(payload or {})
        metadata = dict(metadata or {})

        # CreditAnalysisCompleted v1 -> v2
        if event_type == "CreditAnalysisCompleted" and event_version == 1:
            payload.setdefault("regulatory_basis", [])
            payload.setdefault(
                "explanation",
                f"Auto-generated explanation for confidence={payload.get('confidence')}",
            )
            return payload, 2, metadata

        # DecisionGenerated v1 -> v2
        if event_type == "DecisionGenerated" and event_version == 1:
            inferred_models = metadata.get("contributing_model_versions")
            if not inferred_models:
                inferred_models = {
                    "credit_model": metadata.get("model_version", "unknown")
                }
            payload.setdefault("model_versions", inferred_models)
            return payload, 2, metadata

        # ComplianceChecked v1 -> v2
        if event_type == "ComplianceChecked" and event_version == 1:
            inferred_basis = metadata.get("regulatory_basis_hint")
            if not inferred_basis:
                if payload.get("country") == "US":
                    inferred_basis = "US_REGULATION"
                elif payload.get("country") == "EU":
                    inferred_basis = "EU_REGULATION"
                else:
                    inferred_basis = None
            payload.setdefault("regulatory_basis", inferred_basis)
            return payload, 2, metadata

        # LoanApplicationSubmitted v1 -> v2
        if event_type == "LoanApplicationSubmitted" and event_version == 1:
            inferred_channel = metadata.get("channel") or "unknown"
            payload.setdefault("submission_channel", inferred_channel)
            return payload, 2, metadata

        return payload, event_version, metadata