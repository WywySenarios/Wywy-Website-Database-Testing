from utils import to_lower_snake_case
from wywy_website_types.iterators import iter_tables, iter_descriptors
from wywy_website_types.data import TableInfo, DescriptorInfo
from typing import TypeAlias, Iterator, Callable

"""Return type for endpoint iterator helper functions. Each iteration returns an endpoint and an entry schema."""
EndpointIterator: TypeAlias = Iterator[
    tuple[dict[str, str], TableInfo | DescriptorInfo]
]
EndpointIteratorFactory: TypeAlias = Callable[
    [dict[str, str]],
    EndpointIterator,
]


def table_endpoint_iterator(endpoint_params: dict[str, str]) -> EndpointIterator:
    """Iterates through every endpoint corresponding to a table.

    Args:
        endpoint_params (dict[str, str], optional): Additional endpoint params to inject. Defaults to {}.

    Yields:
        Iterator[endpoint_iterator]:
    """
    for (pretty_database_name, pretty_table_name), table_schema in iter_tables():
        yield {
            "database_name": to_lower_snake_case(pretty_database_name),
            "table_name": to_lower_snake_case(pretty_table_name),
            **endpoint_params,
        }, table_schema


def descriptor_endpoint_iterator(
    endpoint_params: dict[str, str],
) -> EndpointIterator:
    """Iterates through every endpoint corresponding to a descriptor.

    Args:
        endpoint_params (dict[str, str], optional): Additional endpoint params to inject. Defaults to {}.

    Yields:
        Iterator[endpoint_iterator]:
    """
    for (
        pretty_database_name,
        pretty_table_name,
        pretty_descriptor_name,
    ), descriptor_schema in iter_descriptors():
        yield {
            "database_name": to_lower_snake_case(pretty_database_name),
            "table_name": to_lower_snake_case(pretty_table_name),
            "descriptor_name": to_lower_snake_case(pretty_descriptor_name),
            **endpoint_params,
        }, descriptor_schema
