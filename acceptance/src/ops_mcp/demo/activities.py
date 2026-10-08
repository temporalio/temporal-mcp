from temporalio import activity
from temporalio.exceptions import ApplicationError

from ops_mcp.demo.models import OrderInput


@activity.defn
def authorize_payment(order: OrderInput) -> str:
    return f"payment-authorized:{order.order_id}"


@activity.defn
def enrich_product(order: OrderInput) -> str:
    if order.catalog_should_fail:
        raise ApplicationError(
            "503 Service Unavailable from catalog service",
            type="CatalogServiceUnavailable",
        )
    return f"product-enriched:{order.order_id}"
