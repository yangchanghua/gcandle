from gcandle.core.indicator_service.indicator_service import IndicatorService
from gcandle.data.se_types import SeFreq
from gcandle.core.indicator_service.fp_mark_service import FPMarkService

DAY_FP_MARK_SERVICES = []

fpds = [20, 50, 120]
for fpd in fpds:
    DAY_FP_MARK_SERVICES.append(FPMarkService(fpd, SeFreq.DAY))
