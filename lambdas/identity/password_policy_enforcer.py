"""Password strength gate for self-service registration.

Event source: Amazon Cognito user pool trigger, ``PreSignUp_SignUp``.

Scores the proposed password with a Shannon-entropy estimate adjusted for keyboard
walks, character repeats and dictionary fragments derived from the submitted user
attributes, then checks the password's SHA-1 prefix against a breached-hash range
file held in S3 before allowing the sign-up to proceed.
"""

import hashlib
import logging
import math
import os
import re
from typing import Any, Dict, Iterable, List, Set, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BREACH_BUCKET = os.environ.get("BREACH_RANGE_BUCKET", "identity-breach-ranges")
BREACH_PREFIX = os.environ.get("BREACH_RANGE_PREFIX", "sha1/")

MIN_LENGTH = 12
MAX_LENGTH = 160
MIN_ENTROPY_BITS = 55.0
MIN_CHARACTER_CLASSES = 3
MAX_REPEAT_RUN = 3
MAX_KEYBOARD_WALK = 4
BREACH_COUNT_THRESHOLD = 5
ATTRIBUTE_FRAGMENT_MIN = 4

KEYBOARD_ROWS = (
    "`1234567890-=",
    "qwertyuiop[]\\",
    "asdfghjkl;'",
    "zxcvbnm,./",
)
CHARACTER_CLASSES = (
    ("lower", re.compile(r"[a-z]")),
    ("upper", re.compile(r"[A-Z]")),
    ("digit", re.compile(r"[0-9]")),
    ("symbol", re.compile(r"[^A-Za-z0-9]")),
)


class PolicyViolation(Exception):
    """Raised when the proposed password fails a composition or strength rule."""


def _shannon_entropy_bits(password: str) -> float:
    counts: Dict[str, int] = {}
    for character in password:
        counts[character] = counts.get(character, 0) + 1
    total = float(len(password))
    per_symbol = -sum((count / total) * math.log2(count / total) for count in counts.values())
    return per_symbol * total


def _character_classes(password: str) -> Set[str]:
    return {name for name, pattern in CHARACTER_CLASSES if pattern.search(password)}


def _longest_repeat_run(password: str) -> int:
    longest, current = 1, 1
    for index in range(1, len(password)):
        current = current + 1 if password[index] == password[index - 1] else 1
        longest = max(longest, current)
    return longest if password else 0


def _keyboard_positions() -> Dict[str, Tuple[int, int]]:
    return {
        character: (row_index, column_index)
        for row_index, row in enumerate(KEYBOARD_ROWS)
        for column_index, character in enumerate(row)
    }


_POSITIONS = _keyboard_positions()


def _longest_keyboard_walk(password: str) -> int:
    lowered = password.lower()
    longest, current = 1, 1
    for index in range(1, len(lowered)):
        previous = _POSITIONS.get(lowered[index - 1])
        here = _POSITIONS.get(lowered[index])
        if previous and here and previous[0] == here[0] and abs(previous[1] - here[1]) == 1:
            current += 1
            longest = max(longest, current)
        else:
            current = 1
    return longest if lowered else 0


def _attribute_fragments(attributes: Dict[str, Any]) -> List[str]:
    fragments: List[str] = []
    for name, value in attributes.items():
        if not isinstance(value, str) or name.startswith("cognito:"):
            continue
        tokens = re.split(r"[^A-Za-z0-9]+", value.lower())
        fragments.extend(token for token in tokens if len(token) >= ATTRIBUTE_FRAGMENT_MIN)
    return fragments


def _fragment_penalty(password: str, fragments: Iterable[str]) -> Tuple[float, List[str]]:
    lowered = password.lower()
    penalty = 0.0
    hits: List[str] = []
    for fragment in fragments:
        if fragment in lowered:
            penalty += 10.0 + len(fragment)
            hits.append(fragment)
    return penalty, hits


def _breach_count(password: str) -> int:
    digest = hashlib.sha1(password.encode("utf-8")).hexdigest().upper()
    prefix, suffix = digest[:5], digest[5:]
    key = "{0}{1}.txt".format(BREACH_PREFIX, prefix)
    try:
        response = s3.get_object(Bucket=BREACH_BUCKET, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            return 0
        logger.warning("breach_range_unavailable prefix=%s error=%s", prefix, code)
        return 0

    payload = response["Body"].read().decode("utf-8", errors="ignore")
    for line in payload.splitlines():
        candidate, _, count = line.partition(":")
        if candidate.strip().upper() == suffix:
            try:
                return int(count.strip() or "0")
            except ValueError:
                return BREACH_COUNT_THRESHOLD
    return 0


def _evaluate(password: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
    if len(password) < MIN_LENGTH:
        raise PolicyViolation("Password must be at least {0} characters.".format(MIN_LENGTH))
    if len(password) > MAX_LENGTH:
        raise PolicyViolation("Password must be at most {0} characters.".format(MAX_LENGTH))

    classes = _character_classes(password)
    if len(classes) < MIN_CHARACTER_CLASSES:
        raise PolicyViolation(
            "Password must combine at least {0} character classes.".format(MIN_CHARACTER_CLASSES)
        )

    repeat_run = _longest_repeat_run(password)
    if repeat_run > MAX_REPEAT_RUN:
        raise PolicyViolation("Password repeats a character {0} times in a row.".format(repeat_run))

    walk = _longest_keyboard_walk(password)
    if walk > MAX_KEYBOARD_WALK:
        raise PolicyViolation("Password contains a keyboard sequence of {0} keys.".format(walk))

    penalty, hits = _fragment_penalty(password, _attribute_fragments(attributes))
    if hits:
        raise PolicyViolation("Password must not contain your account details.")

    entropy = _shannon_entropy_bits(password) - penalty
    if entropy < MIN_ENTROPY_BITS:
        raise PolicyViolation("Password is too predictable; choose a longer, more varied phrase.")

    breached = _breach_count(password)
    if breached >= BREACH_COUNT_THRESHOLD:
        raise PolicyViolation("Password appears in known breach corpora; choose another.")

    return {
        "entropy_bits": round(entropy, 2), "classes": sorted(classes),
        "repeat_run": repeat_run, "keyboard_walk": walk, "breach_count": breached,
    }


def lambda_handler(event, context):
    request = event.get("request") or {}
    attributes: Dict[str, Any] = dict(request.get("userAttributes") or {})
    validation_data: Dict[str, Any] = dict(request.get("validationData") or {})
    password = str(validation_data.get("password") or request.get("password") or "")
    username = str(event.get("userName", "unknown"))

    if not password:
        logger.info("password_absent_from_trigger username=%s", username)
        raise Exception("Password could not be evaluated. Please retry registration.")

    combined = dict(attributes)
    combined.update({n: v for n, v in validation_data.items() if n != "password"})

    try:
        assessment = _evaluate(password, combined)
    except PolicyViolation as exc:
        logger.info("password_rejected username=%s reason=%s", username, exc)
        raise Exception(str(exc))
    except Exception:
        logger.exception("password_evaluation_failed username=%s", username)
        raise Exception("Password could not be evaluated. Please retry registration.")

    logger.info(
        "password_accepted username=%s entropy=%s classes=%s breach=%s",
        username, assessment["entropy_bits"], len(assessment["classes"]),
        assessment["breach_count"],
    )

    response = dict(event.get("response") or {})
    response["autoConfirmUser"] = bool(validation_data.get("invited") == "true")
    response["autoVerifyEmail"] = "email" in attributes and response["autoConfirmUser"]
    response["autoVerifyPhone"] = False
    event["response"] = response
    return event
