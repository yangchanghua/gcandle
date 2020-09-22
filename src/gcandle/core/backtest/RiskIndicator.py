from gcandle.data.se_types import SeType
from gcandle.objects.data_service_objects import SECURITY_DATA_READ_SERVICE


class RiskIndicator:
    def __init__(self, account):
        self.bm_data = None
        self.bm_code = '000300'
        self.account = account
        self.assets = account.assets

    def _read_bm_data(self):
        self.bm_data = SECURITY_DATA_READ_SERVICE.read_security_data_for_single(
            self.bm_code,
            self.account.start_date,
            self.account.end_date,
            typ=SeType.Index
        )

    def _cacl_bm_assets(self):
        pass

    def benchmark_annualized_returns(self):
        pass

    def annualized_returns(self):
        pass

    def alpha(self):
        pass

    def beta(self):
        pass

    def sharp(self):
        pass
