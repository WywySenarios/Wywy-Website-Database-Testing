import psycopg
from psycopg import sql
from Wywy_Website_Types import DescriptorInfo, TableInfo
from utils import to_lower_snake_case
from .transform import table_transform, TransformTargets
from typing import List, Any


def populate_transformation(cur: psycopg.Cursor, targets: TransformTargets):
    """Populates tables.

    Populates tables with 5 tags,

    Args:
        cur (psycopg.Cursor): The cursor to execute the INSERT querieswith
        targets (TransformTargets): The targets to populate.

    Raises:
        RuntimeError: Raises a runtime error when a data or descriptor table does not have a column schema.
    """
    data_entries: dict[str, DescriptorInfo | TableInfo] = {}
    tags_tables: List[str] = []
    tag_aliases_tables: List[str] = []
    tag_names_tables: List[str] = []
    tag_groups_tables: List[str] = []

    for target_table_name in targets:
        match (targets[target_table_name][0]):
            case "data" | "descriptor":
                schema = targets[target_table_name][1]
                if schema is None:
                    raise RuntimeError(
                        f"Expected column schema but received None: {target_table_name}"
                    )

                data_entries[target_table_name] = schema
            case "tag_aliases":
                tag_aliases_tables.append(target_table_name)
            case "tags":
                tags_tables.append(target_table_name)
            case "tag_names":
                tag_names_tables.append(target_table_name)
            case "tag_groups":
                tag_groups_tables.append(target_table_name)

    for target_table_name in tag_names_tables:
        cur.execute(
            sql.SQL(
                "INSERT INTO {target_table_name} (tag_name) VALUES (%s), (%s), (%s), (%s), (%s);"
            ).format(target_table_name=sql.Identifier(target_table_name)),
            (
                f"{target_table_name} tag 1",
                f"{target_table_name} tag 2",
                f"{target_table_name} tag 3",
                f"{target_table_name} tag 4",
                f"{target_table_name} tag 5",
            ),
        )

    for target_table_name in tag_aliases_tables:
        cur.execute(
            # @TODO more robust foreign keys
            sql.SQL(
                "INSERT INTO {target_table_name} (alias, tag_id) VALUES (%s, 1), (%s, 2), (%s, 3), (%s, 4), (%s, 5);"
            ).format(target_table_name=sql.Identifier(target_table_name)),
            (
                f"{target_table_name} tag alias 1",
                f"{target_table_name} tag alias 2",
                f"{target_table_name} tag alias 3",
                f"{target_table_name} tag alias 4",
                f"{target_table_name} tag alias 5",
            ),
        )

    for target_table_name in tag_groups_tables:
        cur.execute(
            # @TODO more robust foreign keys
            sql.SQL(
                """
                INSERT INTO {target_table_name} (tag_id, group_name)
                VALUES (1, %s), (1, %s), (1, %s), (1, %s), (1, %s),
                    (2, %s), (2, %s), (2, %s), (2, %s),
                    (3, %s), (3, %s), (3, %s),
                    (4, %s), (4, %s),
                    (5, %s);
                """
            ).format(target_table_name=sql.Identifier(target_table_name)),
            (
                f"{target_table_name} group 1",
                f"{target_table_name} group 1",
                f"{target_table_name} group 1",
                f"{target_table_name} group 1",
                f"{target_table_name} group 1",
                f"{target_table_name} group 2",
                f"{target_table_name} group 2",
                f"{target_table_name} group 2",
                f"{target_table_name} group 2",
                f"{target_table_name} group 3",
                f"{target_table_name} group 3",
                f"{target_table_name} group 3",
                f"{target_table_name} group 4",
                f"{target_table_name} group 4",
                f"{target_table_name} group 5",
            ),
        )

    for target_table_name in data_entries:
        columns_shape: List[sql.Composable] = []
        values_shape: List[sql.Composable] = []
        values: List[Any] = []

        if data_entries[target_table_name].get("tagging", False):
            columns_shape.append(sql.Identifier("primary_tag"))
            values_shape.append(sql.Placeholder())
            values.append(1)

        for column_schema in data_entries[target_table_name]["schema"]:
            # values
            # @TODO enum
            match (column_schema["datatype"]):
                case "bool" | "boolean":
                    columns_shape.append(
                        sql.Identifier(to_lower_snake_case(column_schema["name"]))
                    )
                    values_shape.append(sql.Placeholder())
                    values.append("true")
                case "date":
                    columns_shape.append(
                        sql.Identifier(to_lower_snake_case(column_schema["name"]))
                    )
                    values_shape.append(sql.Placeholder())
                    values.append("0001-01-01")
                case "time":
                    columns_shape.append(
                        sql.Identifier(to_lower_snake_case(column_schema["name"]))
                    )
                    values_shape.append(sql.Placeholder())
                    values.append("01:01:01")
                case "timestamp":
                    columns_shape.append(
                        sql.Identifier(to_lower_snake_case(column_schema["name"]))
                    )
                    values_shape.append(sql.Placeholder())
                    values.append("0001-01-01T01:01:01")
                case "int" | "integer":
                    columns_shape.append(
                        sql.Identifier(to_lower_snake_case(column_schema["name"]))
                    )
                    values_shape.append(sql.Placeholder())
                    values.append("23")
                case "float" | "number":
                    columns_shape.append(
                        sql.Identifier(to_lower_snake_case(column_schema["name"]))
                    )
                    values_shape.append(sql.Placeholder())
                    values.append("2.3")
                case "text" | "str" | "string":
                    columns_shape.append(
                        sql.Identifier(to_lower_snake_case(column_schema["name"]))
                    )
                    values_shape.append(sql.Placeholder())
                    values.append(f"{target_table_name} text")
                case "enum":
                    columns_shape.append(
                        sql.Identifier(to_lower_snake_case(column_schema["name"]))
                    )
                    values_shape.append(sql.Placeholder())
                    values.append(None)
                case "geodetic point":
                    columns_shape.append(
                        sql.Identifier(to_lower_snake_case(column_schema["name"]))
                    )
                    columns_shape.append(
                        sql.Identifier(
                            f"{to_lower_snake_case(column_schema["name"])}_latlong_accuracy"
                        )
                    )
                    columns_shape.append(
                        sql.Identifier(
                            f"{to_lower_snake_case(column_schema["name"])}_altitude"
                        )
                    )
                    columns_shape.append(
                        sql.Identifier(
                            f"{to_lower_snake_case(column_schema["name"])}_altitude_accuracy"
                        )
                    )
                    values_shape.append(sql.Placeholder())
                    values_shape.append(sql.Placeholder())
                    values_shape.append(sql.Placeholder())
                    values_shape.append(sql.Placeholder())
                    values.append("POINT (0.2325 0.2325)")
                    values.append(0.23)
                    values.append(2.3)
                    values.append(0.23)

        for _ in range(5):
            cur.execute(
                sql.SQL(
                    "INSERT INTO {target_table_name} ({column_names}) VALUES ({values});"
                ).format(
                    target_table_name=sql.Identifier(target_table_name),
                    column_names=sql.SQL(", ").join(columns_shape),
                    values=sql.SQL(", ").join(values_shape),
                ),
                (*values,),
            )

    for target_table_name in tags_tables:
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {target_table_name} (entry_id, tag_id) VALUES
                    (1, 1), (2, 2), (3, 3), (4, 4), (5, 5),
                    (1, 1), (2, 2), (3, 3), (4, 4),
                    (1, 1), (2, 2), (3, 3),
                    (1, 1), (2, 2),
                    (1, 1);
                """
            ).format(target_table_name=sql.Identifier(target_table_name))
        )


def populate_database():
    """Repopulates the Postgres databases with 1 row of values."""

    table_transform(populate_transformation)
