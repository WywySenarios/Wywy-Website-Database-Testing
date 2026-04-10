"""INSERT related tests for sql-receptionist."""

import unittest
import requests
from wywy_website_types import DataDatatype
from config import CONFIG
from constants import (
    DATA_ENDPOINT,
    TAG_ENDPOINT,
    DESCRIPTOR_ENDPOINT,
    GENERIC_REQUEST_PARAMS,
)
from endpoint_security_tests import test_endpoint_security
from .parameter_requisites_tests import negative_test_endpoint_parameters
from utils import to_lower_snake_case
from ..transformations.purge import purge_database
from ..transformations.populate import (
    create_values,
    populate_database,
)
from typing import Any, Literal


def test_generic_valid_INSERT(
    test_object: unittest.TestCase,
    request_params: dict[str, Any],
) -> None:
    """Tests a valid INSERT.

    Args:
        test_object (unittest.TestCase): The testing object to use.
        payload (dict[str, Any]): A valid payload to INSERT with.
        request_params (dict[str, Any]): The request parameters to use with requests.GET or requests.POST. Must include the URL & the valid payload.
    """

    response = requests.post(
        **request_params,
    )

    test_object.assertEqual(
        response.status_code,
        200,
        f"Valid INSERT to {request_params["url"]} not OK: {response.status_code}: {response.text}",
    )
    test_object.assertEqual(
        int(response.text),
        6,
        "Valid INSERT does not insert into the expected ID.",
    )


def test_empty_insert(
    test_object: unittest.TestCase, request_params: dict[str, Any]
) -> None:
    """Tests empty insertion. Assumes that the given table does not have an empty schema.

    Args:
        test_object (unittest.TestCase): The testing object to use. This may contain the "data" or "json" attribute (they are ignored).
        request_params (dict[str, Any]): The request parameters to use with requests.GET or requests.POST. Must include the URL.
    """

    request_params = {**request_params}
    if "data" in request_params:
        del request_params["data"]
    if "json" in request_params:
        del request_params["json"]

    response = requests.post(
        **request_params,
        data="",
    )
    test_object.assertEqual(
        response.status_code,
        400,
        f"Invalid INSERT (empty body) to {request_params["url"]} did not return status 400: {response.status_code} {response.text}",
    )
    response = requests.post(**request_params, json={})
    test_object.assertEqual(
        response.status_code,
        400,
        f"Invalid INSERT (empty body) to {request_params["url"]} did not return status 400: {response.status_code} {response.text}",
    )


def test_tagging_table(
    test_object: unittest.TestCase,
    payload: dict[str, Any],
    table_type: Literal["tags", "tag_names", "tag_groups", "tag_aliases"],
) -> None:
    """Test a tagging table. Tagging tables have constant payloads during testing, so it's easier to have a generic function do the heavy lifting.

    Args:
        test_object (unittest.TestCase): The testing object to use.

        payload (dict[str, Any]): The payload to use.
    """
    endpoint_params_tested: bool = False
    empty_body_json_checked: bool = False
    endpoint: str = TAG_ENDPOINT.substitute(
        {
            "database_name": to_lower_snake_case(CONFIG["data"][0]["dbname"]),
            "table_name": to_lower_snake_case(
                CONFIG["data"][0]["tables"][0]["tableName"]
            ),
            "table_type": table_type,
        }
    )
    test_endpoint_security(test_object, endpoint)

    for database_info in CONFIG["data"]:
        database_name = to_lower_snake_case(database_info["dbname"])
        for table_info in database_info["tables"]:
            table_name = to_lower_snake_case(table_info["tableName"])
            endpoint = TAG_ENDPOINT.substitute(
                database_name=database_name,
                table_name=table_name,
                table_type=table_type,
            )
            request_params: dict[str, Any] = {
                "url": endpoint,
                **GENERIC_REQUEST_PARAMS,
            }
            response = requests.post(
                **request_params,
                json=payload,
            )
            if table_info.get("tagging", False):
                test_object.assertEqual(
                    response.status_code,
                    200,
                    f"Valid INSERT to {endpoint} is not OK: {response.status_code}: {response.text}",
                )

                # also check for empty body or JSON case if we haven't already done so
                if not empty_body_json_checked:
                    # is INSERTing nothing or an empty JSON caught?
                    response = requests.post(
                        **request_params,
                        data="",
                    )
                    test_object.assertEqual(
                        response.status_code,
                        400,
                        f"Invalid INSERT (empty body) to {endpoint} does not respond with status 400: {response.status_code}: {response.text}",
                    )
                    response = requests.post(
                        **request_params,
                        json={},
                    )
                    test_object.assertEqual(
                        response.status_code,
                        400,
                        f"Invalid INSERT (empty JSON) to {endpoint} does not respond with status 400: {response.status_code}: {response.text}",
                    )
                    empty_body_json_checked = True

                if not endpoint_params_tested:
                    negative_test_endpoint_parameters(
                        test_object,
                        TAG_ENDPOINT,
                        {
                            "database_name": database_name,
                            "table_name": table_name,
                            "table_type": table_type,
                        },
                        "POST",
                        request_params,
                    )
                    endpoint_params_tested = True

            else:
                # check that tables with tagging disabled cannot INSERT tags
                test_object.assertEqual(
                    response.status_code,
                    400,
                    f"Invalid insert (tagging disabled) to {endpoint} did not respond with status 400: {response.status_code}: {response.text}",
                )


class TestSelectEndpoints(unittest.TestCase):
    def setUp(self):
        populate_database()

    def tearDown(self):
        purge_database()

    def test_insert_tags(self):
        """Test the INSERT tags endpoint for every table."""
        test_tagging_table(self, {"tag_id": 1, "entry_id": 1}, "tags")

    def test_insert_tag_names(self):
        """Test the INSERT tag_names endpoint for every table."""
        test_tagging_table(self, {"tag_name": "very unique tag name"}, "tag_names")

    def test_insert_tag_aliases(self):
        """Test the INSERT tag_aliases endpoint for every table."""
        test_tagging_table(
            self, {"alias": "very unique tag alias", "tag_id": 1}, "tag_aliases"
        )

    def test_insert(self):
        """Test the INSERT & UPSERT data (main table & descriptors) endpoint for every table."""
        endpoint: str = DATA_ENDPOINT.substitute(
            {
                "database_name": to_lower_snake_case(CONFIG["data"][0]["dbname"]),
                "table_name": to_lower_snake_case(
                    CONFIG["data"][0]["tables"][0]["tableName"]
                ),
            }
        )
        request_params: dict[str, Any] = {"url": endpoint, **GENERIC_REQUEST_PARAMS}
        test_endpoint_security(self, endpoint)
        data_empty_body_tested: bool = False
        endpoint_params_tested: bool = False
        descriptor_empty_body_tested: bool = False
        descriptor_endpoint_params_tested: bool = False

        payload: dict[str, DataDatatype] = {}
        for database_info in CONFIG["data"]:
            database_name = to_lower_snake_case(database_info["dbname"])
            for table_info in database_info["tables"]:
                table_name = to_lower_snake_case(table_info["tableName"])
                payload = create_values(table_info)
                endpoint = DATA_ENDPOINT.substitute(
                    {"database_name": database_name, "table_name": table_name}
                )
                request_params: dict[str, Any] = {
                    **GENERIC_REQUEST_PARAMS,
                    "url": endpoint,
                    "json": payload,
                }

                test_generic_valid_INSERT(self, request_params)

                # test UPSERT
                request_params["json"]["id"] = 6
                test_generic_valid_INSERT(self, request_params)

                if not endpoint_params_tested:
                    negative_test_endpoint_parameters(
                        self,
                        DATA_ENDPOINT,
                        {"database_name": database_name, "table_name": table_name},
                        "POST",
                        {
                            **request_params,
                        },
                    )
                    endpoint_params_tested = True

                if not data_empty_body_tested:
                    test_empty_insert(self, request_params)
                    data_empty_body_tested = True

                # descriptors
                for descriptor_info in table_info.get("descriptors", []):
                    descriptor_name = to_lower_snake_case(descriptor_info["name"])
                    payload = create_values(descriptor_info)
                    endpoint = DESCRIPTOR_ENDPOINT.substitute(
                        {
                            "database_name": database_name,
                            "table_name": table_name,
                            "descriptor_name": descriptor_name,
                        },
                    )
                    request_params: dict[str, Any] = {
                        **GENERIC_REQUEST_PARAMS,
                        "url": endpoint,
                        "json": payload,
                    }

                    test_generic_valid_INSERT(self, request_params)

                    # test UPSERT
                    request_params["json"]["id"] = 6
                    test_generic_valid_INSERT(self, request_params)

                    if not descriptor_endpoint_params_tested:
                        negative_test_endpoint_parameters(
                            self,
                            DESCRIPTOR_ENDPOINT,
                            {
                                "database_name": database_name,
                                "table_name": table_name,
                                "descriptor_name": descriptor_name,
                            },
                            "POST",
                            {
                                **request_params,
                            },
                        )
                        descriptor_endpoint_params_tested = True

                    if not descriptor_empty_body_tested:
                        test_empty_insert(self, request_params)
                        descriptor_empty_body_tested = True
