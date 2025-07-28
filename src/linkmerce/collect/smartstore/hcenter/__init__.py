from linkmerce.collect import Collector


class PartnerCenter(Collector):
    origin: str = "https://hcenter.shopping.naver.com"
    path: str

    @property
    def url(self) -> str:
        return self.origin + ('/' * (not self.path.startswith('/'))) + self.path
