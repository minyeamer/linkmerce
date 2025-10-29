from __future__ import annotations
from linkmerce.core.searchad.api import NaverSearchAdAPI

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import JsonObject


class TimeContract(NaverSearchAdAPI):
    method = "GET"
    uri = "/ncc/time-contracts"

    @NaverSearchAdAPI.with_session
    def extract(self) -> JsonObject:
        response = self.request_json_safe()
        return self.parse(response)


class BrandNewContract(NaverSearchAdAPI):
    method = "GET"
    uri = "/ncc/brand-new/contracts"

    @NaverSearchAdAPI.with_session
    def extract(self) -> JsonObject:
        response = self.request_json_safe()
        return self.parse(response)
