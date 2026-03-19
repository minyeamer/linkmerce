from __future__ import annotations
from linkmerce.core.searchad.api import NaverSearchAdApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import JsonObject


class TimeContract(NaverSearchAdApi):
    method = "GET"
    uri = "/ncc/time-contracts"

    @NaverSearchAdApi.with_session
    def extract(self) -> JsonObject:
        response = self.request_json_safe()
        return self.parse(response)


class BrandNewContract(NaverSearchAdApi):
    method = "GET"
    uri = "/ncc/brand-new/contracts"

    @NaverSearchAdApi.with_session
    def extract(self) -> JsonObject:
        response = self.request_json_safe()
        return self.parse(response)
