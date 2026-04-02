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

- `DATA_ENDPOINT: string.Template`: template endpoint string for the parent table. Parameters include:
  - database_name
  - table_name
- `DESCRIPTOR_ENDPOINT: string.Template`: template endpoint string for descriptors. Parameters include:
  - database_name
  - table_name
  - descriptor_name
- `TAG_ENDPOINT: string.Template`: template endpoint string for tag tables. Parameters include:
  - database_name
  - table_name
  - table_type
- `AUTH_COOKIES: dict`: authentication cookies

# Tests

- @TODO SELECT table data
- @TODO SELECT descriptor data
- @TODO SELECT tagging data
- @TODO INSERT table data
- @TODO INSERT descriptor data
- @TODO INSERT tagging data
- @TODO UPSERT table data
- @TODO UPSERT descriptor data
- @TODO UPSERT tagging data

## Testing Blindspots

### Config Population Assumption

Assume that the configuration file always has:

- At least one valid database
- At least one valid table per database
- At least one valid descriptor

### "Invalid Values"

It is assumed that concatenating "this string will probably never be valid" to a required endpoint parameter will make the endpoint path invalid (e.g. `database_name + "..."` will be an invalid database name).

### Previously Existent Tables

Tables that were previously existent and now removed from the configuration file are not tested. It is assumed that the test that attempts to SELECT, INSERT, or UPSERT into a non-existent database will cover this blindspot.

### Datatype Testing & Negative INSERT/UPSERT

There is no testing done for datatypes, nor for invalid INSERTs.
