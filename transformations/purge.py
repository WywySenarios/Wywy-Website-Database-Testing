import psycopg
from psycopg import sql
from constants import CONN_CONFIG
from .transform import table_transform, TransformTargets


def purge_transformation(cur: psycopg.Cursor, targets: TransformTargets):
    cur.execute(
        sql.SQL(
            """
            TRUNCATE TABLE {table_names} RESTART IDENTITY CASCADE;
            """
        ).format(
            table_names=sql.SQL(", ").join(
                map(lambda x: sql.Identifier(x), targets.keys())
            )
        )
    )


def purge_database():
    """Purges the Postgres database."""

    table_transform(purge_transformation)

    with psycopg.connect(**CONN_CONFIG, dbname="info", autocommit=True) as conn:
        conn.execute("TRUNCATE sync_status RESTART IDENTITY CASCADE;")
