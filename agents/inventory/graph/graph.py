"""
LangGraph state graph builder for the Inventory & Perishability Agent.
"""

from langgraph.graph import END, StateGraph

from core.models import AgentState
from graph.nodes import (
    load_csv_node,
    sort_by_urgency_node,
    check_perishable_node,
    skip_non_perishable_node,
    compute_expiry_node,
    skip_no_risk_node,
    compute_loss_node,
    assign_urgency_node,
    call_llm_node,
    build_output_node,
    advance_row_node,
    route_perishable,
    route_units_at_risk,
    route_more_rows,
)


def build_graph() -> StateGraph:
    graph = StateGraph(AgentState)

    graph.add_node("load_csv", load_csv_node)
    graph.add_node("sort_by_urgency", sort_by_urgency_node)
    graph.add_node("check_perishable", check_perishable_node)
    graph.add_node("skip_non_perishable", skip_non_perishable_node)
    graph.add_node("compute_expiry", compute_expiry_node)
    graph.add_node("skip_no_risk", skip_no_risk_node)
    graph.add_node("compute_loss", compute_loss_node)
    graph.add_node("assign_urgency", assign_urgency_node)
    graph.add_node("call_llm", call_llm_node)
    graph.add_node("build_output", build_output_node)
    graph.add_node("advance_row", advance_row_node)

    graph.set_entry_point("load_csv")
    graph.add_edge("load_csv", "sort_by_urgency")
    graph.add_edge("sort_by_urgency", "check_perishable")

    graph.add_conditional_edges(
        "check_perishable",
        route_perishable,
        {"compute_expiry": "compute_expiry", "skip_non_perishable": "skip_non_perishable"},
    )

    graph.add_edge("skip_non_perishable", "advance_row")

    graph.add_conditional_edges(
        "compute_expiry",
        route_units_at_risk,
        {"compute_loss": "compute_loss", "skip_no_risk": "skip_no_risk"},
    )

    graph.add_edge("skip_no_risk", "advance_row")
    graph.add_edge("compute_loss", "assign_urgency")
    graph.add_edge("assign_urgency", "call_llm")
    graph.add_edge("call_llm", "build_output")
    graph.add_edge("build_output", "advance_row")

    graph.add_conditional_edges(
        "advance_row",
        route_more_rows,
        {"check_perishable": "check_perishable", END: END},
    )

    return graph.compile()
