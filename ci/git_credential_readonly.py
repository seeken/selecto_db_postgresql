#!/usr/bin/env python3
"""Read the job-scoped credential for exactly the configured Core source."""
import pathlib
import sys


def credential(operation, request, directory=pathlib.Path("/run/selecto-ci/creds")):
    if operation != "get" or request.get("protocol") != "https" or request.get("host") != "github.com":
        return ""
    repository = request.get("path", "").removesuffix(".git")
    if repository != "seeken/selecto":
        return ""
    try:
        if repository not in directory.joinpath("siblings").read_text().splitlines():
            return ""
        token = directory.joinpath("token").read_text().strip()
        if not token or len(token) > 4096 or any(c.isspace() for c in token):
            return ""
    except (OSError, UnicodeError):
        return ""
    return "username=x-access-token\npassword=" + token + "\n"


if __name__ == "__main__":
    request = {}
    size = 0
    for line in sys.stdin:
        size += len(line)
        if size > 16384:
            sys.exit(0)
        line = line.rstrip("\n")
        if not line:
            break
        key, separator, value = line.partition("=")
        if not separator or key in request:
            sys.exit(0)
        request[key] = value
    sys.stdout.write(credential(sys.argv[1] if len(sys.argv) == 2 else "", request))
