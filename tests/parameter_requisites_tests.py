from unittest import TestCase
from string import Template
from requests import get as GET, post as POST
from typing import Literal, Any


def negative_test_endpoint_parameters(
    test_object: TestCase,
    endpoint_template: Template,
    required_parameters: dict[str, str],
    request_method: Literal["GET", "POST"],
    request_params: dict[str, Any],
) -> None:
    """Verifies that all required parameters are indeed required.

    Args:
        test_object (TestCase): The test object to use.
        endpoint_template (Template): The endpoint template to validate.
        required_parameters (dict[str, str]): Valid required templates that would normally produce a 200 result or similar.
        request_method (Literal[&quot;GET&quot;, &quot;POST&quot;]): The request method to test with (GET or POST).
        request_params (dict[str, Any]): Valid request parameters that would normally produce a 200 result or similar. This dictionary will be permutated.
    """

    for required_parameter in required_parameters:
        params = {**required_parameters}
        params[required_parameter] = "this string will probably never be valid"
        endpoint = endpoint_template.substitute(params)
        request_params["url"] = endpoint

        match (request_method):
            case "GET":
                response = GET(**request_params)
            case "POST":
                response = POST(**request_params)

        test_object.assertEqual(
            response.status_code,
            400,
            f"Invalid endpoint access (invalid required parameter). Expected status 400, received {response.status_code}: {response.text}.",
        )
