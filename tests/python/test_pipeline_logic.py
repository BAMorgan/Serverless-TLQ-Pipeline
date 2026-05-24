import csv
import json
import sqlite3
from pathlib import Path

import pytest

import Load
import Query
import Transform


FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
INPUT_CSV = FIXTURES / "sales_input.csv"
EXPECTED_TRANSFORM = FIXTURES / "expected" / "transformed_sales.csv"
EXPECTED_QUERY = FIXTURES / "expected" / "query_response_grouped.json"


def csv_rows(path):
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.reader(handle))


def test_transform_matches_shared_golden_file(tmp_path):
    output = tmp_path / "transformed.csv"

    rows_written, duplicate_rows = Transform.transform(str(INPUT_CSV), str(output))

    assert rows_written == 3
    assert duplicate_rows == 1
    assert csv_rows(output) == csv_rows(EXPECTED_TRANSFORM)


def test_load_creates_expected_sqlite_table(tmp_path):
    db_path = tmp_path / "orders.db"

    rows_inserted = Load.create_database(str(EXPECTED_TRANSFORM), str(db_path))

    assert rows_inserted == 3
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        summary = conn.execute(
            """
            SELECT
                COUNT(*) AS rows,
                SUM(UnitsSold) AS units,
                ROUND(SUM(TotalRevenue), 2) AS revenue,
                ROUND(AVG(GrossMargin), 4) AS margin
            FROM orders
            """
        ).fetchone()
        priorities = [
            row["OrderPriority"]
            for row in conn.execute("SELECT OrderPriority FROM orders ORDER BY OrderID")
        ]

    assert dict(summary) == {"rows": 3, "units": 23, "revenue": 290.0, "margin": 0.4667}
    assert priorities == ["High", "Critical", "Low"]


def test_query_builds_parameterized_grouped_sql():
    query, params, group_columns = Query.build_aggregation_query(
        {"Sales Channel": "Online"},
        ["Region", "Region"],
    )

    assert "WHERE SalesChannel = ?" in query
    assert "GROUP BY Region ORDER BY Region" in query
    assert params == ["Online"]
    assert group_columns == ["Region"]


def test_query_rejects_unsupported_filter_columns():
    with pytest.raises(ValueError, match="Unsupported filter column"):
        Query.build_aggregation_query({"Total Revenue": "100"}, [])


def test_query_filtering_and_grouping_match_shared_expected_response(tmp_path):
    db_path = tmp_path / "orders.db"
    Load.create_database(str(EXPECTED_TRANSFORM), str(db_path))

    query, params, group_columns = Query.build_aggregation_query(
        {"Sales Channel": "Online"},
        ["Region"],
    )
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()

    results = [Query.format_aggregation(row, group_columns) for row in rows]
    actual = {
        "statusCode": 200,
        "aggregations": results[0],
        "results": results,
    }

    expected = json.loads(EXPECTED_QUERY.read_text(encoding="utf-8"))
    assert actual == expected
