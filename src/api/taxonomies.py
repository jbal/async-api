"""API taxonomy classes."""


class HTTPMethod:
    """HTTP method type definition."""

    GET = "get"
    POST = "post"
    PUT = "put"
    DELETE = "delete"
    HEAD = "head"
    OPTIONS = "options"
    PATCH = "patch"


class ContentType:
    """Response content type definition."""

    JSON = 0
    BYTE = 1
    TEXT = 2
