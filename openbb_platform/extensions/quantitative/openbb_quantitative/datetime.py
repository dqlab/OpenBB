"""Lazy bridge to dqlib date and calendar analytics."""

from datetime import date, datetime
from typing import Any

from openbb_core.app.model.obbject import OBBject

from openbb_quantitative._dqlib import (
    domain_dir,
    domain_getattr,
    execute_function,
    to_jsonable,
)
from openbb_quantitative.dqlib_domain import create_domain_router
from openbb_quantitative.models import DQLibScalarResult

_DOMAIN = "datetime"
router = create_domain_router(_DOMAIN)


@router.command(
    methods=["POST"],
    operation_id="dqlib_datetime_simple_year_fraction",
)
def simple_year_fraction(
    start_date: date,
    end_date: date,
    day_count: str = "ACT_365_FIXED",
) -> OBBject[DQLibScalarResult]:
    """Calculate an actual dqlib year fraction for two dates."""
    start = datetime.combine(start_date, datetime.min.time())
    end = datetime.combine(end_date, datetime.min.time())
    result = execute_function(
        _DOMAIN,
        "simple_year_frac_calculator",
        [start, end, day_count],
    )
    return OBBject(results=DQLibScalarResult(value=to_jsonable(result)))


def __getattr__(name: str) -> Any:
    """Resolve a public attribute from dqlib.datetime."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib domain attributes for interactive discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))
