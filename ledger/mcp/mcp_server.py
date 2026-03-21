
from __future__ import annotations
import asyncio
import os
from contextlib import asynccontextmanager

import asyncpg
from dotenv import load_dotenv

load_dotenv()

from ledger.event_store import EventStore
from ledger.upcasters import UpcasterRegistry
from ledger.registry.client import ApplicantRegistryClient
from ledger.mcp.tools import (
    submit_application,
    get_application_status,
    get_event_stream,
    get_decision_history,
    trigger_compliance_check,
    get_agent_performance,
    verify_audit_chain,
    reconstruct_agent_context_tool,
)
from ledger.mcp.resources import (
    get_application_resource,
    get_application_summary_resource,
    get_compliance_resource,
    get_agent_performance_resource,
    get_stream_resource,
    get_health_resource,
)

try:
    from mcp.server.fastmcp import FastMCP
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False
    print("Warning: fastmcp not installed. Run: pip install fastmcp")


DB_URL = os.getenv("DATABASE_URL", "postgresql://ledger_user:ledger123@127.0.0.1:5433/apex_ledger")

# Global state
_store: EventStore | None = None
_pool: asyncpg.Pool | None = None
_registry: ApplicantRegistryClient | None = None


async def get_store() -> EventStore:
    global _store, _pool
    if _store is None:
        upcasters = UpcasterRegistry()
        _store = EventStore(DB_URL, upcaster_registry=upcasters)
        await _store.connect()
    return _store


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DB_URL)
    return _pool


async def get_registry() -> ApplicantRegistryClient:
    global _registry
    if _registry is None:
        pool = await get_pool()
        _registry = ApplicantRegistryClient(pool)
    return _registry


if MCP_AVAILABLE:
    mcp = FastMCP("apex-ledger")

    # ── 8 Tools ───────────────────────────────────────────────────────────────

    @mcp.tool()
    async def tool_submit_application(
        application_id: str,
        applicant_id: str,
        requested_amount_usd: float,
        loan_purpose: str,
        loan_term_months: int = 36,
        submission_channel: str = "web",
        contact_email: str = "",
        contact_name: str = "",
    ) -> dict:
        """Submit a new loan application."""
        store = await get_store()
        registry = await get_registry()
        return await submit_application(
            store, registry, application_id, applicant_id,
            requested_amount_usd, loan_purpose, loan_term_months,
            submission_channel, contact_email, contact_name,
        )

    @mcp.tool()
    async def tool_get_application_status(application_id: str) -> dict:
        """Get current application state from the projection."""
        pool = await get_pool()
        return await get_application_status(pool, application_id)

    @mcp.tool()
    async def tool_get_event_stream(
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> dict:
        """Load full event history for any aggregate stream."""
        store = await get_store()
        return await get_event_stream(store, stream_id, from_position, to_position)

    @mcp.tool()
    async def tool_get_decision_history(application_id: str) -> dict:
        """Get complete decision history for an application."""
        store = await get_store()
        return await get_decision_history(store, application_id)

    @mcp.tool()
    async def tool_trigger_compliance_check(application_id: str) -> dict:
        """Manually trigger a compliance re-evaluation."""
        store = await get_store()
        registry = await get_registry()
        return await trigger_compliance_check(store, registry, application_id)

    @mcp.tool()
    async def tool_get_agent_performance(agent_id: str | None = None) -> dict:
        """Get agent performance metrics."""
        pool = await get_pool()
        return await get_agent_performance(pool, agent_id)

    @mcp.tool()
    async def tool_verify_audit_chain(stream_id: str) -> dict:
        """Verify SHA-256 hash chain integrity of a stream."""
        store = await get_store()
        return await verify_audit_chain(store, stream_id)

    @mcp.tool()
    async def tool_reconstruct_agent_context(
        agent_type: str,
        application_id: str,
    ) -> dict:
        """Reconstruct context for a crashed agent (Gas Town recovery)."""
        store = await get_store()
        return await reconstruct_agent_context_tool(store, agent_type, application_id)

    # ── 6 Resources ───────────────────────────────────────────────────────────

    @mcp.resource("ledger://applications/{application_id}")
    async def resource_application(application_id: str) -> dict:
        """Full application event history across all streams."""
        store = await get_store()
        pool = await get_pool()
        return await get_application_resource(store, pool, application_id)

    @mcp.resource("ledger://applications/{application_id}/summary")
    async def resource_application_summary(application_id: str) -> dict:
        """Current application state from projection."""
        pool = await get_pool()
        return await get_application_summary_resource(pool, application_id)

    @mcp.resource("ledger://applications/{application_id}/compliance")
    async def resource_compliance(application_id: str) -> dict:
        """Compliance audit snapshot for an application."""
        pool = await get_pool()
        return await get_compliance_resource(pool, application_id)

    @mcp.resource("ledger://agents/performance")
    async def resource_agent_performance() -> dict:
        """All agent performance metrics."""
        pool = await get_pool()
        return await get_agent_performance_resource(pool)

    @mcp.resource("ledger://streams/{stream_id}")
    async def resource_stream(stream_id: str) -> dict:
        """Raw event stream by ID."""
        store = await get_store()
        return await get_stream_resource(store, stream_id)

    @mcp.resource("ledger://ledger/health")
    async def resource_health() -> dict:
        """Projection lag and system health."""
        store = await get_store()
        pool = await get_pool()
        return await get_health_resource(store, pool)


if __name__ == "__main__":
    if MCP_AVAILABLE:
        mcp.run()
    else:
        print("Install fastmcp: pip install fastmcp")