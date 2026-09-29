from __future__ import annotations

import os
from datetime import timedelta

import pytest

from ops_mcp.clients import CachingTemporalClientProvider
from ops_mcp.gateways import SdkVisibilityGateway
from ops_mcp.runtime import DemoIdentityProvider, RequestContextFactory
from ops_mcp.targets import YamlTargetCatalog

pytestmark = pytest.mark.skipif(
    os.getenv("OPS_MCP_INTEGRATION") != "1",
    reason="set OPS_MCP_INTEGRATION=1 with the pinned demo server running",
)


async def test_visibility_schema_and_first_demo_page_against_pinned_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPS_MCP_GROUP", "demo-support")
    catalog = YamlTargetCatalog.load(
        os.getenv("OPS_MCP_TARGET_CONFIG", "config/targets.demo.yaml")
    )
    context = RequestContextFactory(
        DemoIdentityProvider(), default_timeout=timedelta(seconds=10)
    ).create(request_id="visibility-integration")
    target = catalog.resolve(context.principal, "orders prod")
    provider = CachingTemporalClientProvider()
    gateway = SdkVisibilityGateway(provider)

    try:
        schema = await gateway.get_legal_search_attributes(context, target)
        page = await gateway.list_workflows_page(
            context,
            target,
            query="OrderId = '88231'",
            page_size=10,
        )
        count = await gateway.count_workflows(
            context,
            target,
            query="WorkflowType = 'OrderWorkflow'",
        )
    finally:
        await provider.shutdown()

    assert any(field.name == "OrderId" for field in schema.fields)
    assert all(row.workflow_id and row.run_id for row in page.rows)
    assert count.count >= len(page.rows)
    assert context.budget.snapshot().temporal_calls == 3
    assert context.budget.snapshot().pages_read == 1
