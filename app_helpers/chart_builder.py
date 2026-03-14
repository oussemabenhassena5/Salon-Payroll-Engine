"""Plotly chart builders for the dashboard."""
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from app_helpers.theme import LOCATION_COLORS, PLOTLY_LAYOUT, PRIMARY, PRIMARY_LIGHT, SUCCESS, WARNING, ERROR, INFO


def _apply_layout(fig: go.Figure, title: str = "") -> go.Figure:
    fig.update_layout(**PLOTLY_LAYOUT, title=dict(text=title, font=dict(size=16)))
    return fig


def revenue_by_location(location_summary: pd.DataFrame) -> go.Figure:
    """Stacked horizontal bar: service/retail/tips per location."""
    if location_summary.empty:
        return go.Figure()
    df = location_summary.sort_values("total_service", ascending=True)
    fig = go.Figure()
    fig.add_trace(go.Bar(y=df["location"], x=df["total_service"], name="Service", orientation="h", marker_color=PRIMARY))
    fig.add_trace(go.Bar(y=df["location"], x=df["total_retail"], name="Retail", orientation="h", marker_color=SUCCESS))
    fig.add_trace(go.Bar(y=df["location"], x=df["total_tips"], name="Tips", orientation="h", marker_color=WARNING))
    fig.update_layout(barmode="stack")
    return _apply_layout(fig, "Revenue by Location")


def commission_donut(final_df: pd.DataFrame) -> go.Figure:
    """Donut chart: service vs retail commission."""
    if final_df.empty:
        return go.Figure()
    svc = final_df["service_comission"].sum() if "service_comission" in final_df.columns else 0
    ret = final_df["total_retail_commission"].sum() if "total_retail_commission" in final_df.columns else 0
    fig = go.Figure(go.Pie(
        labels=["Service Commission", "Retail Commission"],
        values=[svc, ret],
        hole=0.55,
        marker=dict(colors=[PRIMARY, SUCCESS]),
        textinfo="label+percent",
    ))
    return _apply_layout(fig, "Commission Breakdown")


def top_employees(final_df: pd.DataFrame, n: int = 10) -> go.Figure:
    """Horizontal bar: top N employees by total revenue."""
    if final_df.empty:
        return go.Figure()
    df = final_df.copy()
    df["total_revenue"] = df.get("total_service", 0) + df.get("Retail", 0)
    df = df.nlargest(n, "total_revenue").sort_values("total_revenue", ascending=True)
    name_col = "employee" if "employee" in df.columns else "id"
    fig = px.bar(df, x="total_revenue", y=name_col, orientation="h", color_discrete_sequence=[PRIMARY])
    fig.update_traces(texttemplate="$%{x:,.0f}", textposition="outside")
    return _apply_layout(fig, f"Top {n} Employees by Revenue")


def commission_distribution(final_df: pd.DataFrame) -> go.Figure:
    """Histogram of total commission per employee."""
    if final_df.empty:
        return go.Figure()
    df = final_df.copy()
    df["total_commission"] = df.get("service_comission", 0) + df.get("total_retail_commission", 0)
    fig = px.histogram(df, x="total_commission", nbins=20, color_discrete_sequence=[PRIMARY_LIGHT])
    fig.update_layout(xaxis_title="Commission ($)", yaxis_title="# Employees")
    return _apply_layout(fig, "Commission Distribution")


def payroll_cost_by_location_chart(payroll_cost_df: pd.DataFrame) -> go.Figure:
    """Grouped bar: commission, tips, total payroll cost per location."""
    if payroll_cost_df.empty:
        return go.Figure()
    df = payroll_cost_df.sort_values("total_revenue", ascending=True)
    fig = go.Figure()
    fig.add_trace(go.Bar(y=df["location"], x=df["total_commission"], name="Commission", orientation="h", marker_color=PRIMARY))
    fig.add_trace(go.Bar(y=df["location"], x=df["total_tips"], name="Tips", orientation="h", marker_color=WARNING))
    fig.update_layout(barmode="group")
    return _apply_layout(fig, "Payroll Cost by Location")


def period_trend(period_df: pd.DataFrame) -> go.Figure:
    """Dual-axis line: total sales and total commission over periods."""
    if period_df.empty:
        return go.Figure()
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=period_df["period_first"], y=period_df.get("total_service", 0) + period_df.get("total_retail", 0),
        mode="lines+markers", name="Total Sales", line=dict(color=PRIMARY, width=3),
    ))
    fig.add_trace(go.Scatter(
        x=period_df["period_first"], y=period_df.get("total_commission", 0),
        mode="lines+markers", name="Total Commission", line=dict(color=SUCCESS, width=3),
        yaxis="y2",
    ))
    fig.update_layout(
        yaxis=dict(title="Sales ($)"),
        yaxis2=dict(title="Commission ($)", overlaying="y", side="right"),
    )
    return _apply_layout(fig, "Sales & Commission Trend")


def commission_trend(commission_df: pd.DataFrame) -> go.Figure:
    """Line chart of total commission over periods."""
    if commission_df.empty:
        return go.Figure()
    fig = px.line(commission_df, x="period_first", y="total_commission",
                  markers=True, color_discrete_sequence=[PRIMARY])
    fig.update_layout(xaxis_title="Period", yaxis_title="Commission ($)")
    return _apply_layout(fig, "Commission Trend")


def payroll_pct_trend(pct_df: pd.DataFrame) -> go.Figure:
    """Line chart of payroll % of sales with reference line."""
    if pct_df.empty:
        return go.Figure()
    fig = px.line(pct_df, x="period_first", y="payroll_pct_sales",
                  markers=True, color_discrete_sequence=[ERROR])
    fig.add_hline(y=40, line_dash="dash", line_color=WARNING, annotation_text="40% target")
    fig.update_layout(xaxis_title="Period", yaxis_title="Payroll % of Sales")
    return _apply_layout(fig, "Payroll % of Sales")


def exception_trend(exc_df: pd.DataFrame) -> go.Figure:
    """Grouped bar: exception and double-booking counts per period."""
    if exc_df.empty:
        return go.Figure()
    fig = go.Figure()
    fig.add_trace(go.Bar(x=exc_df["period_first"], y=exc_df["exception_count"], name="Exceptions", marker_color=ERROR))
    fig.add_trace(go.Bar(x=exc_df["period_first"], y=exc_df["double_booking_count"], name="Double-Booking", marker_color=WARNING))
    fig.update_layout(barmode="group", xaxis_title="Period", yaxis_title="Count")
    return _apply_layout(fig, "Exception & Double-Booking Trend")


def exception_category_chart(exception_df: pd.DataFrame) -> go.Figure:
    """Horizontal bar of exception count by category."""
    if exception_df.empty or "category" not in exception_df.columns:
        return go.Figure()
    counts = exception_df["category"].value_counts().reset_index()
    counts.columns = ["category", "count"]
    color_map = {
        "Unmatched Employee": ERROR, "Unmapped Service": WARNING,
        "Validation": INFO, "Multi-Location": PRIMARY,
        "Duplicate Record": PRIMARY_LIGHT, "Revenue Outlier": SUCCESS,
    }
    colors = [color_map.get(c, PRIMARY) for c in counts["category"]]
    fig = go.Figure(go.Bar(
        x=counts["count"], y=counts["category"], orientation="h", marker_color=colors,
    ))
    return _apply_layout(fig, "Exceptions by Category")


def employee_history(emp_df: pd.DataFrame) -> go.Figure:
    """Multi-line: service commission and retail commission over periods for one employee."""
    if emp_df.empty:
        return go.Figure()
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=emp_df["period_first"], y=emp_df.get("service_commission", emp_df.get("total_service", 0)),
        mode="lines+markers", name="Service Commission", line=dict(color=PRIMARY, width=2),
    ))
    if "retail_commission" in emp_df.columns:
        fig.add_trace(go.Scatter(
            x=emp_df["period_first"], y=emp_df["retail_commission"],
            mode="lines+markers", name="Retail Commission", line=dict(color=SUCCESS, width=2),
        ))
    fig.update_layout(xaxis_title="Period", yaxis_title="Amount ($)")
    return _apply_layout(fig, "Employee Commission History")
