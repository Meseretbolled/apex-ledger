from __future__ import annotations

from typing import Any, Dict, Tuple


class UpcasterRegistry:
    """
    Central registry for event upcasting.

    Responsibilities:
    - Upgrade older event versions to the latest version
    - Apply inference logic when possible (not just default None)
    - Ensure backward compatibility for stored events
    - Keep read-path transparent (EventStore calls this automatically)

    Returns:
        (payload, new_version, metadata)
    """

    def upcast(
        self,
        event_type: str,
        event_version: int,
        payload: Dict[str, Any],
        metadata: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], int, Dict[str, Any]]:
        payload = dict(payload or {})
        metadata = dict(metadata or {})

        # =========================================================
        # ComplianceChecked v1 → v2
        # Add regulatory_basis (infer if possible)
        # =========================================================
        if event_type == "ComplianceChecked" and event_version == 1:
            inferred_basis = metadata.get("regulatory_basis_hint")

            # inference logic (NOT just None)
            if not inferred_basis:
                if payload.get("country") == "US":
                    inferred_basis = "US_REGULATION"
                elif payload.get("country") == "EU":
                    inferred_basis = "EU_REGULATION"
                else:
                    inferred_basis = None

            payload.setdefault("regulatory_basis", inferred_basis)
            return payload, 2, metadata

        # =========================================================
        # DecisionGenerated v1 → v2
        # Add model_versions (infer from metadata if available)
        # =========================================================
        if event_type == "DecisionGenerated" and event_version == 1:
            inferred_models = metadata.get("contributing_model_versions")

            if not inferred_models:
                # fallback inference
                inferred_models = {
                    "credit_model": metadata.get("model_version", "unknown")
                }

            payload.setdefault("model_versions", inferred_models)
            return payload, 2, metadata

        # =========================================================
        # CreditAnalysisCompleted v1 → v2
        # Add explanation field
        # =========================================================
        if event_type == "CreditAnalysisCompleted" and event_version == 1:
            payload.setdefault(
                "explanation",
                f"Auto-generated explanation for confidence={payload.get('confidence')}",
            )
            return payload, 2, metadata

        # =========================================================
        # LoanApplicationSubmitted v1 → v2
        # Add submission_channel
        # =========================================================
        if event_type == "LoanApplicationSubmitted" and event_version == 1:
            inferred_channel = metadata.get("channel") or "unknown"
            payload.setdefault("submission_channel", inferred_channel)
            return payload, 2, metadata

        # =========================================================
        # Generic fallback (future-proof)
        # =========================================================
        return payload, event_version, metadata