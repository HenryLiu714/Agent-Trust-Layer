"""Seed the agent's own database with the customer a charge belongs to (#85).

    python -m examples.llm_refund_agent.seed CHARGE CUSTOMER_NAME

Setup, not part of a run: `handle_ticket` reads the database and a replay calls it alone, so the
database is made before the agent runs, never by its `main()`. Creates both tables `agent.SCHEMA`
names, in the file `REFUND_DB` names.
"""

import contextlib
import sqlite3
import sys

from examples.llm_refund_agent.agent import SCHEMA, db_path


def seed(path: str, charge: str, name: str) -> None:
    with contextlib.closing(sqlite3.connect(path)) as conn, conn:
        for statement in SCHEMA:
            conn.execute(statement)
        conn.execute(
            "INSERT OR REPLACE INTO customers (charge, name) VALUES (?, ?)", (charge, name)
        )


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(
            "usage: python -m examples.llm_refund_agent.seed CHARGE CUSTOMER_NAME", file=sys.stderr
        )
        return 2
    seed(db_path(), *argv)
    print(f"seeded {db_path()}: {argv[0]} belongs to {argv[1]}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
