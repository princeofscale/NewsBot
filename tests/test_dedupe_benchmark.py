from pathlib import Path

from newsbot.dedupe_benchmark import evaluate


def test_dedupe_benchmark_reports_precision_and_recall() -> None:
    score = evaluate(Path("tests/fixtures/dedupe_pairs.json"))

    assert score.pairs == 4
    assert score.precision == 1
    assert score.recall == 1
