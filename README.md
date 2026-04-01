# Wywy Website Database Testing

This repository contains generic classes and methods for Wywy-Website database API core functionality. It is intended to be used as a submodule.

This repository does not cover API-specific test cases. It only handles the generic functionality that all Wywy-Website database APIs (i.e. cache & master-database) must have.

# Dependencies

This repo needs to be cloned as a submodule inside a folder that also contains:

- Wywy-Website-Types submodule
- `config.py` with the config exported as `CONFIG`
- `endpoint_security_tests.py`
- `constants.py`

## Endpoint security tests

`endpoint_security_tests` should have the following exported members:

- `def test_endpoint_security(test_object: unittest.TestCase, endpoint: str) -> None`

## Constants

The following constants are required:

- `DATA_ENDPOINT: string.Template`: template endpoint string for the parent table
- `DESCRIPTOR_ENDPOINT: string.Template`: template endpoint string for descriptors
- `TAG_ENDPOINT: string.Template`: template endpoint string for tag tables
- `AUTH_COOKIES: dict`: authentication cookies
