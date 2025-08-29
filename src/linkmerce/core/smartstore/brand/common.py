from linkmerce.common.extract import Extractor


class PartnerCenter(Extractor):
    method: str | None = None
    origin: str = "https://hcenter.shopping.naver.com"
    path: str | None = None

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path)
