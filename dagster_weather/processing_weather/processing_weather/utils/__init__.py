# User agent been used to download file
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"


def build_url(protocol: str, base_url: str, *parts: str, params: dict = None) -> str:
    """
    Builds a URL from the given protocol, base URL, additional URL parts, and optional query parameters.

    Supports protocols like http, https, ftp, s3, s3a, s3n, etc.

    Args:
        protocol (str): The protocol to use (e.g., "http", "https", "ftp", "s3", "s3a", "s3n").
        base_url (str): The base URL (e.g., "example.com", "bucket-name").
        *parts (str): Additional parts of the URL path to append (e.g., "api", "v1", "users").
        params (dict, optional): A dictionary of query parameters to append to the URL as key-value pairs.

    Returns:
        str: The fully constructed URL.

    Example:
        >>> build_url("https", "example.com", "api", "v1", "users", params={"id": 123, "name": "John"})
        'https://example.com/api/v1/users?id=123&name=John'

        >>> build_url("s3a", "my-bucket", "path", "to", "file.txt")
        's3a://my-bucket/path/to/file.txt'
    """
    # Ensure the protocol ends with "://"
    if not protocol.endswith("://"):
        protocol += "://"

    # Manually join the base_url and parts for non-HTTP protocols
    if protocol.startswith("s3") or protocol in ("ftp://", "file://"):
        url = protocol + base_url + "/" + "/".join(parts)
    else:
        # For HTTP and HTTPS, use urljoin for standard URL handling
        from urllib.parse import urljoin

        url = protocol + base_url
        for part in parts:
            url = urljoin(url, part)

    # Append query parameters if provided (for HTTP-based URLs)
    if params and protocol in ("http://", "https://"):
        query_string = "&".join(f"{key}={value}" for key, value in params.items())
        url = f"{url}?{query_string}"

    return url
