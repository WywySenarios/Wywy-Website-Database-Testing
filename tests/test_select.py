"""SELECT related tests for sql-receptionist."""

import unittest
import datetime
import re
from requests import get as GET, Response, JSONDecodeError
from config import CONFIG
from Wywy_Website_Types import DataColumn, EntryTableData, DescriptorInfo, TableInfo
from constants import DATA_ENDPOINT, TAG_ENDPOINT, GENERIC_REQUEST_PARAMS
from utils import to_lower_snake_case
from ..transformations.purge import purge_database
from ..transformations.populate import populate_database
from endpoint_security_tests import test_endpoint_security
from .parameter_requisites_tests import negative_test_endpoint_parameters
from typing import List, Any


def assert_data_response(
    test_object: unittest.TestCase,
    response: Response,
    item_schema: DescriptorInfo | TableInfo,
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
    test_object.assertEqual(next(column_name_iterator), "id")

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
        """Test the SELECT data (main table & descriptors) endpoint for every table."""
        endpoint_security_tested: bool = False
        negative_endpoint_paramters_tested: bool = False

        # Can the SQL-receptionist handle empty values?
        for database_schema in CONFIG["data"]:
            database_name = to_lower_snake_case(database_schema["dbname"])
            for table_schema in database_schema["tables"]:
                table_name = to_lower_snake_case(table_schema["tableName"])
                endpoint = DATA_ENDPOINT.substitute(
                    database_name=database_name, table_name=table_name
                )
                request_params: dict[str, Any] = {
                    **GENERIC_REQUEST_PARAMS,
                    "url": endpoint,
                    "params": {"SELECT": "*", "ORDER_BY": "ASC"},
                }

                if not endpoint_security_tested:
                    test_endpoint_security(self, endpoint + "?SELECT=*&ORDER_BY=ASC")
                    endpoint_security_tested = True

                # main data
                response = GET(**request_params)
                assert_data_response(self, response, table_schema)

                # descriptors
                # if "descriptors" in table_schema:
                #     for descriptor_schema in table_schema["descriptors"]:
                #         response = requests.get(
                #             f"{SQL_RECEPTIONIST_URL}/{database_name}/{table_name}/descriptors/{to_lower_snake_case(descriptor_schema["name"])}?SELECT=*&ORDER_BY=ASC",
                #             headers={"Origin": environ["MAIN_URL"]},
                #             cookies=SQL_RECEPTIONIST_AUTH_COOKIES,
                #         )
                #         assert_data_response(self, response, descriptor_schema)

                if not negative_endpoint_paramters_tested:
                    negative_test_endpoint_parameters(
                        self,
                        DATA_ENDPOINT,
                        {
                            "database_name": database_name,
                            "table_name": table_name,
                        },
                        "GET",
                        request_params,
                    )
                    negative_endpoint_paramters_tested = True

        populate_database()

        # @TODO verify invalid URLs

        # Test on a mock dataset
        for database_schema in CONFIG["data"]:
            database_name = to_lower_snake_case(database_schema["dbname"])
            for table_schema in database_schema["tables"]:
                table_name = to_lower_snake_case(table_schema["tableName"])
                endpoint = DATA_ENDPOINT.substitute(
                    database_name=database_name, table_name=table_name
                )
                request_params: dict[str, Any] = {
                    **GENERIC_REQUEST_PARAMS,
                    "url": endpoint,
                    "params": {"SELECT": "*", "ORDER_BY": "ASC"},
                }

                # main data
                response = GET(**request_params)
                assert_data_response(self, response, table_schema)

                # descriptors
                # if "descriptors" in table_schema:
                #     for descriptor_schema in table_schema["descriptors"]:
                #         response = requests.get(
                #             f"{SQL_RECEPTIONIST_URL}/{database_name}/{table_name}/descriptors/{to_lower_snake_case(descriptor_schema["name"])}?SELECT=*&ORDER_BY=ASC",
                #             headers={"Origin": environ["MAIN_URL"]},
                #             cookies=SQL_RECEPTIONIST_AUTH_COOKIES,
                #         )
                #         assert_data_response(self, response, descriptor_schema)

    # def test_select_tags(self):
    #     """Test the SELECT tags endpoint for every table."""

    # def test_select_tag_names(self):
    #     """Test the SELECT tag names endpoint for every table."""

    # def test_select_tag_aliases(self):
    #     """Test the SELECT tag aliases endpoint for every table."""

    # def test_select_tag_groups(self):
    #     """Test the SELECT tag groups endpoint for every table."""
