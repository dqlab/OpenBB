"""OpenBB router for dqlib integration status."""

from openbb_core.app.model.obbject import OBBject
from openbb_core.app.router import Router
from openbb_quantitative._dqlib import get_status
from openbb_quantitative.models import DQLibStatusModel

router = Router(
    prefix="/dqlib",
    description="Optional proprietary dqlib analytics integration.",
)


@router.command(methods=["GET"])
def status() -> OBBject[DQLibStatusModel]:
    """Return dqlib release, runtime compatibility, and availability status."""
    return OBBject(results=DQLibStatusModel.model_validate(get_status()))
