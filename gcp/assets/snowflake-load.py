import os
from pathlib import Path
import snowflake.connector


def run_sql_file(cur, path: Path) -> None:
    sql = path.read_text()
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for stmt in statements:
        print(f"\nRunning {path.name}: {stmt[:100]}...\n")
        cur.execute(stmt)


def validate_staging(cur) -> None:
    cur.execute("SELECT COUNT(*) FROM RIDES_RAW_STAGING")
    row_count = cur.fetchone()[0]
    print(f"RIDES_RAW_STAGING row count: {row_count}")
    if row_count <= 0:
        raise RuntimeError("RIDES_RAW_STAGING is empty")

    cur.execute(
        """
        SELECT COUNT(DISTINCT DATE_TRUNC('month', pickup_datetime))
        FROM RIDES_RAW_STAGING
        """
    )
    month_count = cur.fetchone()[0]
    print(f"RIDES_RAW_STAGING distinct month count: {month_count}")
    if month_count <= 0:
        raise RuntimeError("RIDES_RAW_STAGING contains no pickup months")


def cleanup_staging(cur, snowflake_dir: Path) -> None:
    print("\nCleaning up Snowflake staging objects...\n")
    run_sql_file(cur, snowflake_dir / "cleanup-staging.sql")


def main() -> None:
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )

    snowflake_dir = Path(__file__).resolve().parent.parent / "assets"

    try:
        with conn.cursor() as cur:
            run_sql_file(cur, snowflake_dir / "load-from-gcs.sql")
            run_sql_file(cur, snowflake_dir / "views.sql")
            validate_staging(cur)
            run_sql_file(cur, snowflake_dir / "promote-staging.sql")
            cleanup_staging(cur, snowflake_dir)
    except Exception:
        try:
            with conn.cursor() as cur:
                cleanup_staging(cur, snowflake_dir)
        except Exception as cleanup_error:
            print(f"Failed to clean up staging objects: {cleanup_error}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
