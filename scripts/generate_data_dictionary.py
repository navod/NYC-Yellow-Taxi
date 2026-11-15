"""Generate docs/schema/data_dictionary.csv from sql/schema.sql.

One row per table column: schema, table, column, type, nullable, description.
The description is the inline ``-- comment`` on the column line, if any.

    .venv/bin/python scripts/generate_data_dictionary.py    (or: make docs)
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SQL = ROOT / "sql" / "schema.sql"
OUTPUT = ROOT / "docs" / "schema" / "data_dictionary.csv"

_TABLE = re.compile(
    r"CREATE\s+(?:UNLOGGED\s+)?TABLE\s+IF NOT EXISTS\s+(\w+)\.(\w+)\s*\((.*?)\n\)", re.S
)
_COLUMN = re.compile(r"^(\w+)\s+([A-Z]+(?:\s*\([\d,\s]+\))?)(.*)$")
_CONSTRAINT_WORDS = {"PRIMARY", "UNIQUE", "CHECK", "FOREIGN", "CONSTRAINT"}


def parse(ddl: str) -> list[dict[str, str]]:
    rows = []
    for schema, table, body in _TABLE.findall(ddl):
        for line in body.splitlines():
            code, _, comment = line.partition("--")
            code = code.strip().rstrip(",")
            match = _COLUMN.match(code)
            if not match or match.group(1).upper() in _CONSTRAINT_WORDS:
                continue
            name, data_type, rest = match.groups()
            rest = rest.upper()
            rows.append(
                {
                    "schema": schema,
                    "table": table,
                    "column": name,
                    "type": re.sub(r"\s+", "", data_type),
                    "nullable": "NO" if "NOT NULL" in rest or "PRIMARY KEY" in rest else "YES",
                    "description": comment.strip(),
                }
            )
    return rows


def main() -> None:
    rows = parse(SCHEMA_SQL.read_text())
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} columns to {OUTPUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
