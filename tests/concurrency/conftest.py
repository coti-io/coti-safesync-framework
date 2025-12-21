from __future__ import annotations

import pytest


def _markexpr_allows(config: pytest.Config, marker_name: str) -> bool:
    """
    Return True if the user's `-m` expression *mentions* marker_name.

    This is intentionally a lightweight heuristic: we only need to know whether the
    user intended to run concurrency/demo tests at all.
    """
    expr = getattr(config.option, "markexpr", "") or ""
    # e.g. "concurrency", "concurrency and not demo", "demo"
    return marker_name in expr


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """
    Skip concurrency tests unless explicitly selected with `-m concurrency` (or `-m demo`).
    Skip demo tests unless explicitly selected with `-m demo`.
    """
    allow_concurrency = _markexpr_allows(config, "concurrency") or _markexpr_allows(
        config, "demo"
    )
    allow_demo = _markexpr_allows(config, "demo")

    skip_concurrency = pytest.mark.skip(
        reason="Skipped: run with `pytest -m concurrency` to execute concurrency invariant tests."
    )
    skip_demo = pytest.mark.skip(
        reason="Skipped: run with `pytest -m demo` to execute probabilistic demonstration tests."
    )

    for item in items:
        is_concurrency = item.get_closest_marker("concurrency") is not None
        is_demo = item.get_closest_marker("demo") is not None

        if is_demo and not allow_demo:
            item.add_marker(skip_demo)
            continue

        if is_concurrency and not allow_concurrency:
            item.add_marker(skip_concurrency)


