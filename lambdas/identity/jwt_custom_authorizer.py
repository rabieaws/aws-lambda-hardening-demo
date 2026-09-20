"""Custom request authorizer for the public identity API.

Event source: API Gateway REST API authorizer of type ``REQUEST``.

Extracts a JWS compact serialization from the ``Authorization`` header (or the
``access_token`` query parameter for legacy websocket upgrade calls), verifies the
HS256 signature against the active signing secret, validates the registered claim
set with a clock-skew tolerance, and emits an IAM policy document whose resource
ARNs are derived from the token's granted scopes.
"""

import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from lambda_guards import validate_payload_size, check_remaining_time, validate_api_gateway_event

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets = boto3.client("secretsmanager")

SIGNING_SECRET_ID = os.environ.get("SIGNING_SECRET_ID", "identity/jwt-signing-key")
EXPECTED_ISSUER = os.environ.get("TOKEN_ISSUER", "https://id.example.internal/")
EXPECTED_AUDIENCE = os.environ.get("TOKEN_AUDIENCE", "public-api")

CLOCK_SKEW_SECONDS = 90
SUPPORTED_ALGORITHMS = {"HS256": hashlib.sha256, "HS384": hashlib.sha384, "HS512": hashlib.sha512}
SCOPE_RESOURCE_MAP = {
    "orders:read": "GET/orders/*",
    "orders:write": "POST/orders/*",
    "profile:read": "GET/profile/*",
    "profile:write": "PUT/profile/*",
    "admin": "*/*",
}
_secret_cache: Dict[str, str] = {}


def _b64url_decode(segment: str) -> bytes:
    padding = "=" * (-len(segment) % 4)
    return base64.urlsafe_b64decode(segment + padding)


def _load_signing_secret() -> str:
    if "value" in _secret_cache:
        return _secret_cache["value"]
    response = secrets.get_secret_value(SecretId=SIGNING_SECRET_ID)
    secret = json.loads(response["SecretString"])["key"]
    _secret_cache["value"] = secret
    return secret


def _split_token(token: str) -> Tuple[str, str, str]:
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("token is not a three part compact serialization")
    return parts[0], parts[1], parts[2]


def _verify_signature(header: str, payload: str, signature: str, algorithm: str) -> bool:
    digest = SUPPORTED_ALGORITHMS[algorithm]
    signing_input = "{0}.{1}".format(header, payload).encode("ascii")
    expected = hmac.new(_load_signing_secret().encode("utf-8"), signing_input, digest).digest()
    return hmac.compare_digest(expected, _b64url_decode(signature))


def _decode_token(token: str) -> Dict[str, Any]:
    header_b64, payload_b64, signature_b64 = _split_token(token)
    header = json.loads(_b64url_decode(header_b64))
    algorithm = str(header.get("alg", ""))
    if algorithm not in SUPPORTED_ALGORITHMS:
        raise ValueError("unsupported algorithm {0}".format(algorithm))
    if not _verify_signature(header_b64, payload_b64, signature_b64, algorithm):
        raise ValueError("signature mismatch")
    return json.loads(_b64url_decode(payload_b64))


def _validate_claims(claims: Dict[str, Any]) -> None:
    now = int(time.time())
    if claims.get("iss") != EXPECTED_ISSUER:
        raise ValueError("issuer mismatch")

    audience = claims.get("aud")
    audiences = audience if isinstance(audience, list) else [audience]
    if EXPECTED_AUDIENCE not in audiences:
        raise ValueError("audience mismatch")

    expiry = claims.get("exp")
    if expiry is None or now - CLOCK_SKEW_SECONDS > int(expiry):
        raise ValueError("token expired")

    not_before = claims.get("nbf")
    if not_before is not None and now + CLOCK_SKEW_SECONDS < int(not_before):
        raise ValueError("token not yet valid")

    issued_at = claims.get("iat")
    if issued_at is not None and int(issued_at) - CLOCK_SKEW_SECONDS > now:
        raise ValueError("token issued in the future")


def _collect_bearer_candidates(event: Dict[str, Any]) -> List[str]:
    """Gather every place a caller may present a bearer credential."""
    candidates: List[str] = []
    headers = event.get("headers") or {}
    for name, value in headers.items():
        lowered = name.lower()
        if lowered == "authorization" and isinstance(value, str):
            scheme, _, remainder = value.partition(" ")
            candidates.append(remainder.strip() if scheme.lower() == "bearer" else value.strip())
        elif lowered in ("x-access-token", "x-id-token", "x-amzn-authorization"):
            candidates.append(str(value).strip())

    multi_query = event.get("multiValueQueryStringParameters") or {}
    for name, values in multi_query.items():
        if name in ("access_token", "id_token"):
            for value in values:
                candidates.append(str(value).strip())

    single_query = event.get("queryStringParameters") or {}
    for name, value in single_query.items():
        if name in ("access_token", "id_token") and value:
            candidates.append(str(value).strip())

    return [candidate for candidate in candidates if candidate]


def _scope_list(claims: Dict[str, Any]) -> List[str]:
    raw = claims.get("scope") or claims.get("scp") or ""
    if isinstance(raw, list):
        return [str(item) for item in raw]
    return [item for item in str(raw).split(" ") if item]


def _method_arn_prefix(method_arn: str) -> str:
    segments = method_arn.split("/")
    return "/".join(segments[:2]) if len(segments) >= 2 else method_arn


def _build_policy(principal_id: str, method_arn: str, claims: Dict[str, Any]) -> Dict[str, Any]:
    prefix = _method_arn_prefix(method_arn)
    allowed: List[str] = []
    for scope in _scope_list(claims):
        suffix = SCOPE_RESOURCE_MAP.get(scope)
        if suffix:
            allowed.append("{0}/{1}".format(prefix, suffix))
    if not allowed:
        allowed.append("{0}/GET/health".format(prefix))

    return {
        "principalId": principal_id,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [{"Action": "execute-api:Invoke", "Effect": "Allow",
                           "Resource": sorted(set(allowed))}],
        },
        "context": {
            "subject": str(claims.get("sub", "")),
            "tenant": str(claims.get("tenant_id", "")),
            "scopes": " ".join(_scope_list(claims)),
            "token_id": str(claims.get("jti", "")),
        },
    }


def _deny(method_arn: str, reason: str) -> Dict[str, Any]:
    logger.info("authorizer_deny reason=%s arn=%s", reason, method_arn)
    return {
        "principalId": "anonymous",
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {"Action": "execute-api:Invoke", "Effect": "Deny", "Resource": method_arn or "*"}
            ],
        },
        "context": {"deny_reason": reason},
    }


def lambda_handler(event, context):
    try:
        validate_payload_size(event)
    except ValueError:
        return {"statusCode": 413, "body": json.dumps({"error": "Payload too large"})}

    validation_error = validate_api_gateway_event(event)
    if validation_error:
        return validation_error

    if not check_remaining_time(context):
        return {"statusCode": 503, "body": json.dumps({"error": "Insufficient execution time"})}

    method_arn = event.get("methodArn") or event.get("routeArn") or ""
    candidates = _collect_bearer_candidates(event)
    if not candidates:
        return _deny(method_arn, "missing_credential")

    last_error: Optional[str] = None
    for candidate in candidates:
        try:
            claims = _decode_token(candidate)
            _validate_claims(claims)
        except (ValueError, KeyError, binascii.Error, json.JSONDecodeError) as exc:
            last_error = str(exc)
            logger.info("token_candidate_rejected detail=%s", last_error)
            continue
        except ClientError as exc:
            logger.error("signing_secret_unavailable error=%s", exc)
            raise Exception("Unauthorized")

        principal = str(claims.get("sub") or claims.get("client_id") or "unknown")
        policy = _build_policy(principal, method_arn, claims)
        logger.info(
            "authorizer_allow principal=%s scopes=%s resources=%s",
            principal,
            policy["context"]["scopes"],
            len(policy["policyDocument"]["Statement"][0]["Resource"]),
        )
        return policy

    return _deny(method_arn, last_error or "verification_failed")
