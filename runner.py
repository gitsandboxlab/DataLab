import pyodbc

def copy_view_to_table(
    src_engine,
    dest_engine,
    src_view: str,
    dest_table: str,
    truncate: bool = False,
    chunk_size: int = 50000
):
    """
    Copy data from SQL Server view to SQL Server table using SQLAlchemy engines + pyodbc.

    Parameters:
        src_engine: SQLAlchemy engine (source DB)
        dest_engine: SQLAlchemy engine (target DB)
        src_view: e.g. dbo.vw_Sales
        dest_table: e.g. dbo.Sales
        truncate: if True, truncates destination table first
        chunk_size: number of rows per batch
    """

    # Get raw DBAPI connections from SQLAlchemy engines
    src_conn = src_engine.raw_connection()
    dest_conn = dest_engine.raw_connection()

    src_cursor = src_conn.cursor()
    dest_cursor = dest_conn.cursor()

    dest_cursor.fast_executemany = True

    try:
        # Optional truncate
        if truncate:
            dest_cursor.execute(f"TRUNCATE TABLE {dest_table}")
            dest_conn.commit()

        # Pull from source view
        src_cursor.execute(f"SELECT * FROM {src_view}")

        # Get column names
        columns = [col[0] for col in src_cursor.description]

        insert_sql = f"""
            INSERT INTO {dest_table} ({",".join(columns)})
            VALUES ({",".join(["?"] * len(columns))})
        """

        total = 0

        while True:
            batch = src_cursor.fetchmany(chunk_size)
            if not batch:
                break

            dest_cursor.executemany(insert_sql, batch)
            dest_conn.commit()

            total += len(batch)
            print(f"Inserted {total} rows into {dest_table}")

        print(f"DONE: {total} rows copied from {src_view} → {dest_table}")

    finally:
        src_cursor.close()
        dest_cursor.close()
        src_conn.close()
        dest_conn.close()
