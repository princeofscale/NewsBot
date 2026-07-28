import json
from dataclasses import dataclass
from pathlib import Path

from newsbot.dedupe import same_event
from newsbot.schemas import StrictModel


class DedupePair(StrictModel):
    left: str
    right: str
    same_event: bool


@dataclass(slots=True)
class DedupeScore:
    pairs: int
    precision: float
    recall: float
    false_positives: int
    false_negatives: int


def evaluate(path: Path) -> DedupeScore:
    pairs = [DedupePair.model_validate(item) for item in json.loads(path.read_text())]
    true_positive = false_positive = false_negative = 0
    for pair in pairs:
        predicted = same_event(pair.left, pair.right)
        true_positive += predicted and pair.same_event
        false_positive += predicted and not pair.same_event
        false_negative += not predicted and pair.same_event
    return DedupeScore(
        pairs=len(pairs),
        precision=(
            true_positive / (true_positive + false_positive)
            if true_positive + false_positive
            else 1
        ),
        recall=(
            true_positive / (true_positive + false_negative)
            if true_positive + false_negative
            else 1
        ),
        false_positives=false_positive,
        false_negatives=false_negative,
    )
