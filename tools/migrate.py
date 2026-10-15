"""Apply clickhouse/migrations/NNNN_*.sql in order, once each.

    python -m tools.migrate [--host localhost --port 8123 --user otel --password otel --database otel]
    python -m tools.migrate --status

Applied migrations are recorded in `schema_migrations` with a checksum; editing an applied migration is an
error (write a new one instead).
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR = ROOT / "clickhouse" / "migrations"
_NAME = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode()).hexdigest()

    def statements(self, database: str = "otel") -> list[str]:
        """`{{database}}` in a migration is replaced by the target database (dictionary sources need it)."""
        return split_statements(self.sql.replace("{{database}}", database))


def discover(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    found: list[Migration] = []
    for path in sorted(directory.glob("*.sql")):
        m = _NAME.match(path.name)
        if not m:
            raise ValueError(f"bad migration file name: {path.name} (expected NNNN_snake_case.sql)")
        found.append(Migration(int(m.group(1)), path.name, path.read_text(encoding="utf-8")))
    versions = [m.version for m in found]
    if len(set(versions)) != len(versions):
        raise ValueError(f"duplicate migration versions in {directory}")
    return found


def split_statements(sql: str) -> list[str]:
    """Split on `;` outside quotes and comments. Comments are kept with the statement that follows."""
    out: list[str] = []
    buf: list[str] = []
    i, n = 0, len(sql)
    quote: str | None = None
    while i < n:
        c = sql[i]
        if quote:
            buf.append(c)
            if c == "\\" and i + 1 < n:
                buf.append(sql[i + 1])
                i += 2
                continue
            if c == quote:
                quote = None
        elif c in ("'", '"', "`"):
            quote = c
            buf.append(c)
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            end = n if end == -1 else end
            buf.append(sql[i:end])
            i = end
            continue
        elif sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            end = n if end == -1 else end + 2
            buf.append(sql[i:end])
            i = end
            continue
        elif c == ";":
            stmt = "".join(buf).strip()
            if _has_code(stmt):
                out.append(stmt)
            buf = []
        else:
            buf.append(c)
        i += 1
    tail = "".join(buf).strip()
    if _has_code(tail):
        out.append(tail)
    return out


def _has_code(stmt: str) -> bool:
    without = re.sub(r"--[^\n]*|/\*.*?\*/", "", stmt, flags=re.S)
    return bool(without.strip())


def connect(args: argparse.Namespace):  # type: ignore[no-untyped-def]
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=args.host, port=args.port, username=args.user, password=args.password, database=args.database
    )


def applied(client) -> dict[int, str]:  # type: ignore[no-untyped-def]
    client.command(
        "CREATE TABLE IF NOT EXISTS schema_migrations "
        "(version UInt32, name String, checksum String, applied_at DateTime DEFAULT now()) "
        "ENGINE = MergeTree ORDER BY version"
    )
    rows = client.query("SELECT version, checksum FROM schema_migrations").result_rows
    return {int(v): str(c) for v, c in rows}


def migrate(client, migrations: list[Migration], database: str = "otel") -> list[str]:  # type: ignore[no-untyped-def]
    done = applied(client)
    ran: list[str] = []
    for m in migrations:
        if m.version in done:
            if done[m.version] != m.checksum:
                raise RuntimeError(f"{m.name} was edited after being applied; add a new migration instead")
            continue
        for stmt in m.statements(database):
            client.command(stmt)
        client.insert(
            "schema_migrations",
            [[m.version, m.name, m.checksum]],
            column_names=["version", "name", "checksum"],
        )
        ran.append(m.name)
    return ran


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default=os.environ.get("CLICKHOUSE_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")))
    p.add_argument("--user", default=os.environ.get("CLICKHOUSE_USER", "otel"))
    p.add_argument("--password", default=os.environ.get("CLICKHOUSE_PASSWORD", "otel"))
    p.add_argument("--database", default=os.environ.get("CLICKHOUSE_DB", "otel"))
    p.add_argument("--status", action="store_true")
    args = p.parse_args(argv)

    client = connect(args)
    migrations = discover()
    if args.status:
        done = applied(client)
        for m in migrations:
            print(f"{'applied' if m.version in done else 'pending'}  {m.name}")
        return 0
    ran = migrate(client, migrations, args.database)
    print("\n".join(f"applied {name}" for name in ran) or "up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
