from gcandle.data.se_types import SeFreq, SeType

class SecurityFetcher:
    def __init__(self):
        pass

    def fetch_bars(self, typ: SeType, code: str, start: str, end: str, freq: SeFreq):
        pass

    def fetch_xdxr(self, code):
        pass

    def fetch_security_list(self):
        pass
