import os
import traceback
from typing import Any, Dict, Optional, Tuple

import snowflake.connector
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("Snowflake (preconfigured queries, SSO-only)", json_response=True)

# -----------------------------
# Preconfigured SQL (NO raw SQL)
# -----------------------------
# Notes:
# - Snowflake comments use -- or /* ... */ (NOT //)
# - Avoid referring to SELECT aliases in the same SELECT (fixed below)
# - Uses qmark bindings (?) with strict parameter validation in Python

QUERIES: Dict[str, str] = {
    "basic_query": r"""
SELECT
  CURRENT_USER()      AS user,
  CURRENT_ROLE()      AS role,
  CURRENT_WAREHOUSE() AS warehouse,
  CURRENT_DATABASE()  AS database,
  CURRENT_SCHEMA()    AS schema;
""",
}

# -----------------------------
# Helpers
# -----------------------------
def _require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise KeyError(f"Missing required environment variable: {name}")
    return v

def _connect():
    """
    SSO-only connection:
      - Uses authenticator=externalbrowser
      - Never passes a password (prevents "empty password" issues)
    """
    authenticator = os.environ.get("SNOWFLAKE_AUTHENTICATOR", "externalbrowser")
    if authenticator != "externalbrowser":
        raise ValueError(
            f"SSO-only server: SNOWFLAKE_AUTHENTICATOR must be 'externalbrowser' (got '{authenticator}')"
        )

    # IMPORTANT: account should be like "uipath_observability.west-europe.azure"
    # (no .snowflakecomputing.com)
    conn_kwargs = {
        "account": "uipath_observability.west-europe.azure",
        "user": "anurag.krishna@uipath.com",
        "warehouse": "PROD_CUSTOMER_READ",
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        "database": os.environ.get("SNOWFLAKE_DATABASE"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA"),
        "authenticator": authenticator,
        "client_session_keep_alive": True,
    }

    # Remove None/empty optionals
    conn_kwargs = {k: v for k, v in conn_kwargs.items() if v}

    return snowflake.connector.connect(**conn_kwargs)

def _normalize_ilike(pattern: Optional[Any]) -> Optional[str]:
    if pattern is None:
        return None
    p = str(pattern).strip()
    if not p:
        return None
    # If caller gives "salesforce", we turn it into "%salesforce%"
    if "%" not in p:
        p = f"%{p}%"
    return p


# -----------------------------
# MCP tools
# -----------------------------
@mcp.tool()
def list_saved_queries() -> dict:
    """List IDs of allowed, pre-configured queries."""
    return {
        "queries": sorted(QUERIES.keys()),
        "params_schema": {
            "month": "int (required, 1..12)",
            "year": "int (required, 2000..2100)",
            "connector_ilike": "string (optional, e.g. 'salesforce' or '%sales%')",
        },
    }

def _params_for(query_id: str, params: Optional[Dict[str, Any]]) -> Optional[list]:
    """
    Returns the binds for a given query_id and params.
    """
    return None


@mcp.tool()
def run_saved_query(
    query_id: str,
    params: Optional[Dict[str, Any]] = None,
    max_rows: int = 500
) -> dict:
    """
    Run a pre-configured query by ID (NO raw SQL).
    """
    if query_id not in QUERIES:
        return {"ok": False, "error": f"Unknown query_id '{query_id}'", "allowed": sorted(QUERIES.keys())}

    sql = QUERIES[query_id]
    max_rows = max(1, min(int(max_rows), 5000))

    try:
        binds = None
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, binds)
                cols = [c[0] for c in cur.description] if cur.description else []
                rows = cur.fetchmany(max_rows)
                return {
                    "ok": True,
                    "query_id": query_id,
                    "params_used": params or {},
                    "columns": cols,
                    "rows": rows,
                    "row_count_returned": len(rows),
                    "row_limit": max_rows,
                }
    except Exception as e:
        return {
            "ok": False,
            "query_id": query_id,
            "error_type": type(e).__name__,
            "error": str(e),
            "traceback_tail": traceback.format_exc().splitlines()[-20:],
        }

if __name__ == "__main__":
    mcp.run(transport="stdio")
