"""
Generate a Copernicus Data Space Ecosystem (CDSE) access token.

Follows official docs:
- grant_type=password
- client_id=cdse-public
- username/password
- optional totp for 2FA-enabled accounts
"""
import argparse
import os
import sys
import requests
from dotenv import load_dotenv


AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"


def request_token(username: str, password: str, totp: str | None = None) -> dict:
    data = {
        "client_id": "cdse-public",
        "grant_type": "password",
        "username": username,
        "password": password,
    }
    if totp:
        data["totp"] = totp

    response = requests.post(AUTH_URL, data=data, timeout=30)
    if response.status_code != 200:
        detail = response.text.strip().replace("\n", " ")
        raise RuntimeError(f"Token request failed ({response.status_code}): {detail}")
    return response.json()


def main() -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Get CDSE access token")
    parser.add_argument("--username", default=os.getenv("COPERNICUS_USER"))
    parser.add_argument("--password", default=os.getenv("COPERNICUS_PASS"))
    parser.add_argument("--totp", default=os.getenv("COPERNICUS_TOTP"))
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print full token response JSON (contains sensitive values).",
    )
    args = parser.parse_args()

    if not args.username or not args.password:
        print("Missing credentials.")
        print("Provide --username/--password or set COPERNICUS_USER/COPERNICUS_PASS in .env")
        return 1

    try:
        token_data = request_token(args.username, args.password, args.totp)
    except Exception as exc:
        print(f"Error: {exc}")
        print("If your account has 2FA, provide --totp or set COPERNICUS_TOTP.")
        return 1

    access_token = token_data.get("access_token")
    expires_in = token_data.get("expires_in")
    token_type = token_data.get("token_type", "Bearer")

    if args.raw:
        print(token_data)
        return 0

    # Print the token to stdout so callers can pipe/capture it cleanly.
    # All surrounding informational text goes to stderr to avoid polluting
    # captured output and to reduce accidental token exposure in CI logs.
    _err = sys.stderr
    print("Token generated successfully.", file=_err)
    print(f"token_type: {token_type}", file=_err)
    print(f"expires_in: {expires_in} seconds", file=_err)
    print("WARNING: treat the token below as a secret — do not log or share it.", file=_err)
    print(f"Authorization: {token_type} <token>", file=_err)
    # Only the raw token goes to stdout for easy capture: $(get_cdse_token ...)
    print(access_token)
    return 0


if __name__ == "__main__":
    sys.exit(main())
