from linkmerce.common.extract import Extractor


class PartnerCenter(Extractor):
    origin: str = "https://hcenter.shopping.naver.com"
    path: str

    @property
    def url(self) -> str:
        return self.origin + ('/' * (not self.path.startswith('/'))) + self.path
