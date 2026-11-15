"""Create the Superset dashboard through Superset's REST API (dashboard as code).

Run inside the Superset container (setup.sh does this):

    docker compose exec superset python /app/bootstrap/bootstrap_dashboard.py

What it builds:
  1. a database connection to the taxi warehouse
  2. one virtual dataset per file in sql/analytics/ - the same SQL the
     Streamlit dashboard runs, so both dashboards always agree
  3. charts on top of those datasets
  4. a single dashboard that lays the charts out

The script is idempotent: every object is looked up by name first and only
created when missing, so it is safe to run on every setup.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
ADMIN_USER = os.environ["SUPERSET_ADMIN_USER"]
ADMIN_PASSWORD = os.environ["SUPERSET_ADMIN_PASSWORD"]
WAREHOUSE_URI = os.environ["WAREHOUSE_SQLALCHEMY_URI"]
ANALYTICS_SQL_DIR = Path(os.environ.get("ANALYTICS_SQL_DIR", "/app/sql/analytics"))

DATABASE_NAME = "Taxi Warehouse"
DASHBOARD_TITLE = "NYC Yellow Taxi - Trip Analytics"
DASHBOARD_SLUG = "nyc-taxi"
DATASETS = [
    "kpi_summary",
    "peak_hours",
    "revenue_by_payment_type",
    "avg_fare_per_mile",
    "avg_fare_per_mile_by_borough",
    "pipeline_runs",
]


# ------------------------------------------------------------------ API client
class SupersetClient:
    """Thin wrapper over the REST API: login once, then find-or-create objects."""

    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

        login = self.session.post(
            f"{self.base_url}/api/v1/security/login",
            json={"username": username, "password": password, "provider": "db", "refresh": True},
            timeout=30,
        )
        login.raise_for_status()
        self.session.headers["Authorization"] = f"Bearer {login.json()['access_token']}"

        # Write requests must also carry a CSRF token.
        csrf = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/", timeout=30)
        csrf.raise_for_status()
        self.session.headers["X-CSRFToken"] = csrf.json()["result"]
        self.session.headers["Referer"] = self.base_url

    def request(self, method: str, path: str, **kwargs) -> dict:
        response = self.session.request(
            method, f"{self.base_url}/api/v1/{path}", timeout=120, **kwargs
        )
        if not response.ok:
            raise RuntimeError(f"{method} {path} failed ({response.status_code}): {response.text}")
        return response.json()

    def find_id(self, resource: str, column: str, value: str) -> int | None:
        """Return the id of the object whose ``column`` equals ``value``, if any."""
        query = {"filters": [{"col": column, "opr": "eq", "value": value}]}
        result = self.request("GET", f"{resource}/", params={"q": json.dumps(query)})["result"]
        return result[0]["id"] if result else None

    def find_or_create(self, resource: str, column: str, value: str, payload: dict) -> int:
        existing = self.find_id(resource, column, value)
        if existing is not None:
            print(f"  exists   {resource}: {value}")
            return existing
        created = self.request("POST", f"{resource}/", json=payload)["id"]
        print(f"  created  {resource}: {value}")
        return created


# ---------------------------------------------------------------- chart params
def sum_of(column: str, label: str) -> dict:
    """A Superset metric: SUM(column). Datasets are already aggregated to one
    row per category, so SUM simply passes the pre-computed value through."""
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": "SUM",
        "label": label,
    }


def big_number(column: str, label: str, number_format: str) -> dict:
    return {
        "viz_type": "big_number_total",
        "metric": sum_of(column, label),
        "y_axis_format": number_format,
        "header_font_size": 0.4,
    }


def bar_chart(x_column: str, y_column: str, y_label: str, number_format: str) -> dict:
    return {
        "viz_type": "echarts_timeseries_bar",
        "x_axis": x_column,
        "xAxisForceCategorical": True,
        "x_axis_sort_asc": True,
        "metrics": [sum_of(y_column, y_label)],
        "groupby": [],
        "y_axis_format": number_format,
        "show_legend": False,
        "row_limit": 1000,
    }


def table(columns: list[str], newest_first_by: str | None = None) -> dict:
    params = {
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": columns,
        "row_limit": 50,
        "include_search": False,
    }
    if newest_first_by:
        # Superset expects each sort as a JSON string: [column, is_ascending].
        params["order_by_cols"] = [json.dumps([newest_first_by, False])]
    return params


# (chart name, dataset, chart params, width out of 12, height in grid units)
CHARTS = [
    ("Total trips", "kpi_summary", big_number("total_trips", "Trips", ",d"), 3, 20),
    ("Total revenue", "kpi_summary", big_number("total_revenue", "Revenue", "$,.0f"), 3, 20),
    ("Avg fare per mile", "kpi_summary", big_number("avg_fare_per_mile", "USD", "$,.2f"), 3, 20),
    ("Avg trip distance (mi)", "kpi_summary", big_number("avg_trip_miles", "Miles", ",.2f"), 3, 20),
    (
        "Peak hours: trips by pickup hour",
        "peak_hours",
        bar_chart("pickup_hour", "trips", "Trips", ",d"),
        12,
        50,
    ),
    (
        "Total revenue by payment type",
        "revenue_by_payment_type",
        bar_chart("payment_type_name", "total_revenue", "Revenue", "$,.0f"),
        6,
        50,
    ),
    (
        "Avg fare per mile by pickup borough",
        "avg_fare_per_mile_by_borough",
        bar_chart("borough", "avg_fare_per_mile", "Fare per mile", "$,.2f"),
        6,
        50,
    ),
    (
        "Avg fare per mile by month",
        "avg_fare_per_mile",
        table(["year", "month_name", "trips", "avg_fare_per_mile"]),
        4,
        40,
    ),
    (
        "Pipeline runs (dw.etl_run_audit)",
        "pipeline_runs",
        table(
            [
                "data_month",
                "step",
                "status",
                "started_at",
                "duration_s",
                "rows_in",
                "rows_out",
                "rows_rejected",
            ],
            newest_first_by="started_at",
        ),
        8,
        40,
    ),
]


def build_layout(charts: list[tuple[int, str, int, int]]) -> dict:
    """Build Superset's dashboard layout: charts flow left to right into rows
    of a 12-column grid. ``charts`` holds (chart id, name, width, height)."""
    layout: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}},
    }
    row_id, used_width = None, 12  # forces a new row for the first chart

    for chart_id, name, width, height in charts:
        if used_width + width > 12:
            row_id = f"ROW-{len(layout['GRID_ID']['children']) + 1}"
            layout["GRID_ID"]["children"].append(row_id)
            layout[row_id] = {
                "type": "ROW",
                "id": row_id,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            used_width = 0

        component_id = f"CHART-{chart_id}"
        layout[row_id]["children"].append(component_id)
        layout[component_id] = {
            "type": "CHART",
            "id": component_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {"chartId": chart_id, "sliceName": name, "width": width, "height": height},
        }
        used_width += width

    return layout


# ------------------------------------------------------------------------ main
def main() -> None:
    client = SupersetClient(SUPERSET_URL, ADMIN_USER, ADMIN_PASSWORD)

    print("Database")
    database_id = client.find_or_create(
        "database",
        "database_name",
        DATABASE_NAME,
        {"database_name": DATABASE_NAME, "sqlalchemy_uri": WAREHOUSE_URI, "expose_in_sqllab": True},
    )

    print("Datasets")
    dataset_ids = {}
    for name in DATASETS:
        # A virtual dataset is a saved query; Superset wraps it as a subquery,
        # so the trailing semicolon has to go.
        query = (ANALYTICS_SQL_DIR / f"{name}.sql").read_text().strip().rstrip(";")
        dataset_ids[name] = client.find_or_create(
            "dataset",
            "table_name",
            name,
            {"database": database_id, "schema": "dw", "table_name": name, "sql": query},
        )
        # Sync column names and types from the query result.
        client.request("PUT", f"dataset/{dataset_ids[name]}/refresh")

    print("Dashboard")
    dashboard_id = client.find_or_create(
        "dashboard",
        "slug",
        DASHBOARD_SLUG,
        {"dashboard_title": DASHBOARD_TITLE, "slug": DASHBOARD_SLUG, "published": True},
    )

    print("Charts")
    placed = []
    for name, dataset, params, width, height in CHARTS:
        dataset_id = dataset_ids[dataset]
        chart_id = client.find_or_create(
            "chart",
            "slice_name",
            name,
            {
                "slice_name": name,
                "viz_type": params["viz_type"],
                "datasource_id": dataset_id,
                "datasource_type": "table",
                "params": json.dumps({**params, "datasource": f"{dataset_id}__table"}),
                "dashboards": [dashboard_id],
            },
        )
        placed.append((chart_id, name, width, height))

    client.request(
        "PUT",
        f"dashboard/{dashboard_id}",
        json={"position_json": json.dumps(build_layout(placed))},
    )
    print(f"Dashboard ready: /superset/dashboard/{DASHBOARD_SLUG}/")


if __name__ == "__main__":
    try:
        main()
    except (requests.RequestException, RuntimeError) as exc:
        sys.exit(f"Superset bootstrap failed: {exc}")
