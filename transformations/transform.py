from config import CONFIG
from constants import CONN_CONFIG
from utils import to_lower_snake_case
from Wywy_Website_Types import TableType, DescriptorInfo, TableInfo
import psycopg
from typing import Callable, TypeAlias, Literal

TransformTargets: TypeAlias = dict[
    str, tuple[TableType, DescriptorInfo | TableInfo | None]
]
DataTransformTargets: TypeAlias = dict[
    str, tuple[Literal["data", "descriptor"], DescriptorInfo | TableInfo]
]


def table_transform(
    transformation: Callable[
        [psycopg.Cursor, TransformTargets],
        None,
    ],
) -> None:
    """Applies a transformation to every table. The cursor does not have autocommit.

    Args:
        transformation (Callable[[psycopg.Cursor, TransformTargets], None]): The transformation to apply to every item inside the CONFIG. The transformation function takes in in the cursor and a dictionary with the database table name as the key and a tuple with length 2 containing the table type and the full item schema if available.
    """

    for databaseSchema in CONFIG["data"]:
        with psycopg.connect(
            **CONN_CONFIG,
            dbname=to_lower_snake_case(databaseSchema["dbname"]),
            autocommit=False,
        ) as conn:
            with conn.cursor() as cur:
                targets: TransformTargets = {}
                for tableSchema in databaseSchema["tables"]:
                    table_name: str = to_lower_snake_case(tableSchema["tableName"])

                    # main table
                    targets[table_name] = ("data", tableSchema)

                    # tagging tables
                    if tableSchema.get("tagging", False) is True:
                        targets[f"{table_name}_tags"] = ("tags", None)
                        targets[f"{table_name}_tag_aliases"] = ("tag_aliases", None)
                        targets[f"{table_name}_tag_groups"] = ("tag_groups", None)
                        targets[f"{table_name}_tag_names"] = ("tag_names", None)

                    # descriptors
                    for descriptorSchema in tableSchema.get("descriptors", []):
                        targets[
                            f"{table_name}_{to_lower_snake_case(descriptorSchema['name'])}_descriptors"
                        ] = (
                            "descriptor",
                            descriptorSchema,
                        )  # @TODO check if this table type name is correct

                # call transformation
                transformation(
                    cur,
                    targets,
                )


def entry_table_transform(
    transformation: Callable[
        [psycopg.Cursor, DataTransformTargets],
        None,
    ],
) -> None:
    """Applies a transformation to every entry table. The cursor does not have autocommit.

    Args:
        transformation (Callable[ [psycopg.Cursor, DataTransformTargets], None, ]): The transformation to apply.  The transformation function takes in in the cursor and a dictionary with the database table name as the key and a tuple with length 2 containing the table type and the full item schema.
    """
    for databaseSchema in CONFIG["data"]:
        with psycopg.connect(
            **CONN_CONFIG,
            dbname=to_lower_snake_case(databaseSchema["dbname"]),
            autocommit=False,
        ) as conn:
            with conn.cursor() as cur:
                targets: DataTransformTargets = {}
                for tableSchema in databaseSchema["tables"]:
                    table_name: str = to_lower_snake_case(tableSchema["tableName"])

                    # main table
                    targets[table_name] = ("data", tableSchema)

                    # descriptors
                    for descriptorSchema in tableSchema.get("descriptors", []):
                        targets[
                            f"{table_name}_{to_lower_snake_case(descriptorSchema['name'])}_descriptors"
                        ] = (
                            "descriptor",
                            descriptorSchema,
                        )  # @TODO check if this table type name is correct

                # call transformation
                transformation(
                    cur,
                    targets,
                )
