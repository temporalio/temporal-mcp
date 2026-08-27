import asyncio

from ops_mcp.server import mcp, resolve_target


def test_registers_exact_session_one_tools() -> None:
    tools = asyncio.run(mcp.list_tools())
    assert {tool.name for tool in tools} == {
        "resolve_target",
        "filter_workflows",
        "diagnose_workflow",
        "count_workflows",
        "plan_workflow_reset",
    }
    assert all(tool.annotations.readOnlyHint for tool in tools)


async def test_resolve_target_hides_connection_material() -> None:
    result = await resolve_target("orders prod")
    assert result["target_ref"] == "orders-prod"
    assert result["namespace"] == "default"
    assert "address" not in result
    assert "api_key" not in result
