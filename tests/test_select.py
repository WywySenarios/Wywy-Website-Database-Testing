"""SELECT related tests for sql-receptionist."""

from string import Template
import unittest
import datetime
import re
from requests import get as GET, Response, JSONDecodeError
from generic_database_api.endpoint_iterator import (
    table_endpoint_iterator,
    descriptor_endpoint_iterator,
    EndpointIteratorFactory,
)
from config import CONFIG
from wywy_website_types import DataColumn, EntryTableData, DescriptorInfo, TableInfo
from constants import DATA_ENDPOINT, TAG_ENDPOINT, GENERIC_REQUEST_PARAMS
from utils import to_lower_snake_case
from ..transformations.purge import purge_database
from ..transformations.populate import populate_database
from endpoint_security_tests import test_endpoint_security
from .parameter_requisites_tests import negative_test_endpoint_parameters
from typing import List, Any, Literal

# START - tagging table schemas (they are not real descriptors)
TAGS_SCHEMA: DescriptorInfo = {
    "name": "",
    "schema": [
        {
            "name": "entry_id",
            "datatype": "int",
            "entrytype": "none",
        },
        {"name": "tag_id", "datatype": "int", "entrytype": "none"},
    ],
}

TAG_NAMES_SCHEMA: DescriptorInfo = {
    "name": "",
    "schema": [{"name": "tag_name", "datatype": "text", "entrytype": "none"}],
}

TAG_ALIASES_SCHEMA: DescriptorInfo = {
    "name": "",
    "schema": [{"name": "tag_id", "datatype": "int", "entrytype": "none"}],
}

TAG_GROUPS_SCHEMA: DescriptorInfo = {
    "name": "",
    "schema": [
        {"name": "tag_id", "datatype": "int", "entrytype": "none"},
        {"name": "group_name", "datatype": "text", "entrytype": "none"},
    ],
}
# END - tagging table schemas


def test_select_endpoint(
    test_object: unittest.TestCase,
    endpoint_iterator_factory: EndpointIteratorFactory,
    endpoint_template: Template,
    endpoint_params: dict[str, Any],
    request_params: dict[str, Any],
    data_response_params: dict[str, Any] = {},
):
    """Runs relevant SELECT test cases on the given endpoints.
    * endpoint security tests
    * negative request parameter tests
    * SELECT test

    It is assumed that populate_database() has not yet been called.

    This function does not clean up database values after itself.

    Args:
        test_object (unittest.TestCase): The test object to use.
        endpoint_iterator (endpoint_iterator): The endpoint iterator to use.
        endpoint_template (Template): The endpoint template to use during negative request parameter tests.
        endpoint_params (dict[str, Any]): The endpoint params to use during negative request parameter tests.
        request_params (dict[str, str]): Request parameters (e.g. headers, cookies).
        data_response_params (dict[str, str], Optional): Additional parameters to pass into assert_data_response. Defaults to {}.
    """
    endpoint_security_tested: bool = False
    negative_endpoint_paramters_tested: bool = False

    # test SELECTing empty values
    for computed_endpoint_params, entry_schema in endpoint_iterator_factory(
        endpoint_params
    ):
        endpoint = endpoint_template.substitute(computed_endpoint_params)
        computed_request_params: dict[str, Any] = {**request_params, "url": endpoint}

        if not endpoint_security_tested:
            test_endpoint_security(test_object, endpoint)
            endpoint_security_tested = True

        # main data
        response = GET(**computed_request_params)
        assert_data_response(
            test_object, response, entry_schema, **data_response_params
        )

        if not negative_endpoint_paramters_tested:
            negative_test_endpoint_parameters(
                test_object,
                endpoint_template,
                computed_endpoint_params,
                "GET",
                computed_request_params,
            )
            negative_endpoint_paramters_tested = True

    populate_database()

    for computed_endpoint_params, entry_schema in endpoint_iterator_factory(
        endpoint_params
    ):
        endpoint = endpoint_template.substitute(computed_endpoint_params)
        computed_request_params: dict[str, Any] = {**request_params, "url": endpoint}

        # main data
        response = GET(**computed_request_params)
        assert_data_response(
            test_object, response, entry_schema, **data_response_params
        )


def test_tagging_endpoint(
    test_object: unittest.TestCase,
    table_type: Literal["tags", "tag_aliases", "tag_groups", "tag_names"],
    table_schema: DescriptorInfo,
):
    """Test a tagging endpoint.

    Args:
        test_object (unittest.TestCase): The testing object to use.
        table_type (Literal[&quot;tags&quot;, &quot;tag_aliases&quot;, &quot;tag_groups&quot;, &quot;tag_names&quot;]): The table_type to test.
        table_schema (DescriptorInfo): A mock schema of the table to test.
    """
    id_column_name = "alias" if table_type == "tag_aliases" else "id"

    endpoint_security_tested: bool = False
    negative_endpoint_paramters_tested: bool = False

    # test SELECTing empty values
    for database_schema in CONFIG["data"]:
        database_name = to_lower_snake_case(database_schema["dbname"])
        for parent_table_schema in database_schema["tables"]:
            table_name = to_lower_snake_case(parent_table_schema["tableName"])
            endpoint = TAG_ENDPOINT.substitute(
                database_name=database_name,
                table_name=table_name,
                table_type=table_type,
            )
            request_params: dict[str, Any] = {
                **GENERIC_REQUEST_PARAMS,
                "url": endpoint,
                "params": {"SELECT": "*", "ORDER_BY": "ASC"},
            }

            if not endpoint_security_tested:
                test_endpoint_security(test_object, endpoint + "?SELECT=*&ORDER_BY=ASC")
                endpoint_security_tested = True

            # main data
            response = GET(**request_params)
            assert_data_response(
                test_object, response, table_schema, id_column_name=id_column_name
            )

            if not negative_endpoint_paramters_tested:
                negative_test_endpoint_parameters(
                    test_object,
                    TAG_ENDPOINT,
                    {
                        "database_name": database_name,
                        "table_name": table_name,
                        "table_type": table_type,
                    },
                    "GET",
                    request_params,
                )
                negative_endpoint_paramters_tested = True

    populate_database()

    for database_schema in CONFIG["data"]:
        database_name = to_lower_snake_case(database_schema["dbname"])
        for parent_table_schema in database_schema["tables"]:
            table_name = to_lower_snake_case(parent_table_schema["tableName"])
            endpoint = TAG_ENDPOINT.substitute(
                database_name=database_name,
                table_name=table_name,
                table_type=table_type,
            )
            request_params: dict[str, Any] = {
                **GENERIC_REQUEST_PARAMS,
                "url": endpoint,
                "params": {"SELECT": "*", "ORDER_BY": "ASC"},
            }

            # main data
            response = GET(**request_params)
            assert_data_response(
                test_object, response, table_schema, id_column_name=id_column_name
            )


def assert_data_response(
    test_object: unittest.TestCase,
    response: Response,
    item_schema: DescriptorInfo | TableInfo,
    id_column_name: str = "id",
    expected_num_rows: int | None = None,
) -> EntryTableData:
    column_schema: List[DataColumn] = item_schema["schema"]

    test_object.assertEqual(
        response.status_code,
        200,
        f"Data fetch to {response.url} response not OK: {response.text}",
    )

    try:
        data = response.json()
    except JSONDecodeError as e:
        test_object.fail(
            f"""
Failed to decode JSON:
--------exception--------
{e}
------response.text------
{response.text}
---repr(response.text)---
{repr(response.text)}
-------------------------
            """
        )

    test_object.assertIsInstance(data, dict, "Data fetch response is not a dictionary")

    # check keys
    test_object.assertCountEqual(
        data,
        ["columns", "data"],
        "Data fetch response must only contain columns and data of interest.",
    )

    # check column names
    test_object.assertIsInstance(data["columns"], list)
    column_name_iterator = iter(data["columns"])
    # ID column
    test_object.assertEqual(next(column_name_iterator), id_column_name)

    # primary_tag
    if item_schema.get("tagging", False):
        test_object.assertEqual(next(column_name_iterator), "primary_tag")

    for column in column_schema:
        column_name = to_lower_snake_case(column["name"])
        test_object.assertEqual(next(column_name_iterator), column_name)

        match (column["datatype"]):
            case "geodetic point":
                test_object.assertEqual(
                    next(column_name_iterator),
                    f"{column_name}_latlong_accuracy",
                    f"Missing sub-column {column_name}_latlong_accuracy",
                )
                test_object.assertEqual(
                    next(column_name_iterator),
                    f"{column_name}_altitude",
                    f"Missing sub-column {column_name}_altitude",
                )
                test_object.assertEqual(
                    next(column_name_iterator),
                    f"{column_name}_altitude_accuracy",
                    f"Missing sub-column {column_name}_altitude_accuracy",
                )
            case _:
                pass

        if column.get("comments", False) is True:
            test_object.assertEqual(
                next(column_name_iterator), f"{column_name}_comments"
            )
    test_object.assertTrue(
        not any(column_name_iterator), "Extra columns are not allowed."
    )

    # check data
    test_object.assertIsInstance(data["data"], list)
    for row in data["data"]:
        test_object.assertIsInstance(row, list)

        row_iterator = iter(row)

        # skip ID column
        next(row_iterator)

        # primary_tag
        if item_schema.get("tagging", False):
            # @TODO validate primary_tag value
            next(row_iterator)

        for i in range(len(column_schema)):
            # assume it is impossible for the sql-receptionist to select the wrong table's data
            # @TODO schema check submodule
            match (column_schema[i]["datatype"]):
                case "bool" | "boolean":
                    test_object.assertIn(str(row[i]).lower(), ["true", "false"])
                # test will fail if the string is unparseable or not in an expected format
                case "int" | "integer":
                    int(next(row_iterator))
                case "float" | "number":
                    float(next(row_iterator))
                case "str" | "string" | "text":
                    test_object.assertTrue(
                        next(row_iterator), "String should not be empty."
                    )
                case "date":
                    datetime.date.fromisoformat(next(row_iterator))
                case "time":
                    datetime.time.fromisoformat(next(row_iterator))
                case "timestamp":
                    datetime.datetime.fromisoformat(next(row_iterator))
                case "enum":
                    # @TODO enums
                    next(row_iterator)
                    pass
                case "geodetic point":
                    point = next(row_iterator)
                    test_object.assertIsInstance(
                        point,
                        str,
                        "Geodetic points must be represented in PostGIS WKT.",
                    )
                    matches = re.fullmatch(
                        r"POINT ?\((-?\d+(?:\.\d+)?) (-?\d+(?:\.\d+)?)\)",
                        point,
                    )

                    # check longitude (X) and latitude (Y)
                    if matches:
                        test_object.assertIsNotNone(
                            matches.group(1),
                            "Geodetic points must be represented in PostGIS WKT.",
                        )
                        test_object.assertIsNotNone(
                            matches.group(2),
                            "Geodetic points must be represented in PostGIS WKT.",
                        )
                        longitude = float(matches.group(1))
                        test_object.assertGreaterEqual(
                            longitude,
                            -180,
                            "Invalid longitude: Geodetic points must be represented in PostGIS WKT.",
                        )
                        test_object.assertLessEqual(
                            longitude,
                            180,
                            "Invalid longitude: Geodetic points must be represented in PostGIS WKT.",
                        )
                        latitude = float(matches.group(1))
                        test_object.assertGreaterEqual(
                            latitude,
                            -90,
                            "Invalid latitude: Geodetic points must be represented in PostGIS WKT.",
                        )
                        test_object.assertLessEqual(
                            latitude,
                            90,
                            "Invalid latitude: Geodetic points must be represented in PostGIS WKT.",
                        )
                    else:
                        test_object.assertIsNotNone(
                            matches,
                            "Geodetic points must be represented in PostGIS WKT.",
                        )

                    latlong_accuracy = next(row_iterator)

                    if latlong_accuracy is not None:
                        float(latlong_accuracy)

                    altitude = next(row_iterator)
                    if altitude is not None:
                        float(altitude)

                    altitude_accuracy = next(row_iterator)
                    if altitude_accuracy is not None:
                        float(altitude_accuracy)

            if column_schema[i].get("comments", False) is True:
                # @TODO generate & test comments values
                test_object.assertEqual(next(row_iterator), "")

        test_object.assertTrue(not any(row_iterator), "Excess data.")

    return data


class TestSelectEndpoints(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        purge_database()

    def test_select(self):
        """Test the SELECT data (main tables) endpoint for every table."""
        test_select_endpoint(
            self,
            table_endpoint_iterator,
            DATA_ENDPOINT,
            {},
            {
                **GENERIC_REQUEST_PARAMS,
                "params": {"SELECT": "*", "ORDER_BY": "ASC"},
            },
        )

    # def test_select_tags(self):
    #     """Test the SELECT tags endpoint for every table."""
    #     test_tagging_endpoint(self, "tags", TAGS_SCHEMA)

    def test_select_tag_names(self):
        """Test the SELECT tag names endpoint for every table."""
        test_tagging_endpoint(self, "tag_names", TAG_NAMES_SCHEMA)

    def test_select_tag_aliases(self):
        """Test the SELECT tag aliases endpoint for every table."""
        test_tagging_endpoint(self, "tag_aliases", TAG_ALIASES_SCHEMA)

    def test_select_tag_groups(self):
        """Test the SELECT tag groups endpoint for every table."""
        test_tagging_endpoint(self, "tag_groups", TAG_GROUPS_SCHEMA)
