import re
from difflib import SequenceMatcher


def normalize_title(title: str) -> str:
    return " ".join(re.findall(r"[\w]+", title.casefold()))


def title_similarity(left: str, right: str) -> float:
    return SequenceMatcher(None, normalize_title(left), normalize_title(right)).ratio()


def event_similarity(left: str, right: str, threshold: float = 0.55) -> float:
    left_tokens = set(normalize_title(left).split())
    right_tokens = set(normalize_title(right).split())
    action_overlap = len(left_tokens & right_tokens) >= 3
    score = title_similarity(left, right)
    return score if action_overlap and score >= threshold else 0.0


def same_event(left: str, right: str, threshold: float = 0.55) -> bool:
    return event_similarity(left, right, threshold) > 0
