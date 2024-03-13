"""API taxonomy classes."""


class HTTPMethod:

    GET = "get"
    POST = "post"
    PUT = "put"
    DELETE = "delete"
    HEAD = "head"
    OPTIONS = "options"
    PATCH = "patch"


class ContentType:

    JSON = 0
    BYTE = 1
    TEXT = 2
