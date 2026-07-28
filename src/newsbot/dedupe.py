import re
from datetime import datetime, timedelta
from difflib import SequenceMatcher

ACTION_GROUPS = (
    {"отключ", "приостанов", "перекро"},
    {"дтп", "авари", "столк", "сбил"},
    {"пожар", "возгоран", "горел"},
    {"откры", "запуст", "начал работу"},
    {"закры", "прекрат"},
    {"ремонт", "отремонт", "восстанов"},
    {"осуд", "приговор", "суд"},
    {"задерж", "арест"},
    {"обруш", "рухнул"},
)
STREET_RE = re.compile(
    r"\b(?:улиц(?:а|е|ы|у|ей)?|ул\.|проспект(?:е|а|у|ом)?|пр-т|"
    r"переул(?:ок|ке|ка|ку|ком)?|шоссе|площад(?:ь|и))\s+"
    r"([А-Яа-яЁё0-9][А-Яа-яЁё0-9.-]*(?:\s+[А-Яа-яЁё0-9][А-Яа-яЁё0-9.-]*){0,2}?)"
    r"(?:\s*,?\s*(?:дом|д\.)\s*№?\s*(\d+[А-Яа-я]?)(?:[/к]\s*(\d+))?)?"
    r"(?=\s*(?:[.;:]|$))"
    r"|\b(?:на|по)\s+([А-ЯЁ][а-яё-]{4,}(?:ского|цкого|ской|цкой|ова|ева|ина)?)"
)
ENTITY_RE = re.compile(r"\b[А-ЯЁ][а-яё-]{2,}(?:\s+[А-ЯЁ][а-яё-]{2,}){0,2}\b")
NUMBER_RE = re.compile(r"\b\d+(?:[.,]\d+)?\b")


def normalize_title(title: str) -> str:
    return " ".join(re.findall(r"[\w]+", title.casefold()))


def title_similarity(left: str, right: str) -> float:
    return SequenceMatcher(None, normalize_title(left), normalize_title(right)).ratio()


def extract_addresses(value: str) -> set[str]:
    addresses = set()
    for match in STREET_RE.finditer(value):
        street, house, building, inferred = match.groups()
        if inferred:
            addresses.add(inferred.casefold())
        elif street:
            addresses.add(
                " ".join(
                    part.casefold() for part in (street, house, building) if part
                )
            )
    return addresses


def _actions(value: str) -> set[int]:
    normalized = normalize_title(value)
    return {
        index
        for index, stems in enumerate(ACTION_GROUPS)
        if any(stem in normalized for stem in stems)
    }


def extract_entities(value: str) -> set[str]:
    return {match.group().casefold() for match in ENTITY_RE.finditer(value)}


def event_similarity(
    left: str,
    right: str,
    threshold: float = 0.62,
    *,
    left_text: str = "",
    right_text: str = "",
    left_time: datetime | None = None,
    right_time: datetime | None = None,
) -> float:
    if left_time and right_time and abs(left_time - right_time) > timedelta(hours=48):
        return 0.0

    left_context = f"{left} {left_text[:500]}"
    right_context = f"{right} {right_text[:500]}"
    left_addresses = extract_addresses(left_context)
    right_addresses = extract_addresses(right_context)
    if left_addresses and right_addresses and left_addresses.isdisjoint(right_addresses):
        return 0.0

    left_actions = _actions(left_context)
    right_actions = _actions(right_context)
    sequence = title_similarity(left, right)
    if left_actions and right_actions and left_actions.isdisjoint(right_actions):
        return 0.0
    if not (left_actions & right_actions) and sequence < 0.9:
        return 0.0

    left_tokens = set(normalize_title(left).split())
    right_tokens = set(normalize_title(right).split())
    union = left_tokens | right_tokens
    token_score = len(left_tokens & right_tokens) / len(union) if union else 0.0

    left_numbers = set(NUMBER_RE.findall(left_context))
    right_numbers = set(NUMBER_RE.findall(right_context))
    number_score = (
        len(left_numbers & right_numbers) / len(left_numbers | right_numbers)
        if left_numbers and right_numbers
        else 0.0
    )
    left_entities = extract_entities(left_context)
    right_entities = extract_entities(right_context)
    entity_score = (
        len(left_entities & right_entities) / len(left_entities | right_entities)
        if left_entities or right_entities
        else 0.0
    )
    address_score = 1.0 if left_addresses and left_addresses == right_addresses else 0.0
    score = (
        sequence * 0.5
        + token_score * 0.25
        + (1.0 if left_actions & right_actions else 0.0) * 0.1
        + address_score * 0.05
        + number_score * 0.05
        + entity_score * 0.05
    )
    return score if score >= threshold else 0.0


def same_event(left: str, right: str, threshold: float = 0.62) -> bool:
    return event_similarity(left, right, threshold) > 0
