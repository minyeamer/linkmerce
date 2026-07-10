from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Account(DuckDBTransformer):
    """사방넷 쇼핑몰로그인 메뉴의 쇼핑몰 계정 목록을 변환 및 적재하는 클래스.

    - **Extractor**: `Account`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: sabangnet_account`
    """

    extractor = "Account"
    tables = {"table": "sabangnet_account"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data",
        fields = [
            "shmaId", "shmaNm", "olMktTydvsDivCd", "olMktTydvsDivNm", "shmaUrlAddr", "scmUrlAddr",
            "corpNm", "sortSrno", "useYn", "shmaCnctnLoginId", "acntRegsSrno", "ecptPwd"
        ],
    )


class ShopNormal(DuckDBTransformer):
    """사방넷 쇼핑몰관리(일반) 메뉴의 일반 쇼핑몰 목록을 변환 및 적재하는 클래스.

    - **Extractor**: `ShopNormal`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: sabangnet_shop_normal`
    """

    extractor = "ShopNormal"
    tables = {"table": "sabangnet_shop_normal"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data",
        fields = [
            "shmaId", "fstRegsDt", "shmaNm", "rpstNm", "olMktTydvsDivCd", "olMktTydvsDivNm",
            "sortSrno", "exclFormSrno", "shmaExpoYn"
        ],
    )

    def transform(self, obj, **kwargs):
        return super().transform(obj, **kwargs)


class AccountNormal(DuckDBTransformer):
    """사방넷 쇼핑몰 정보 보기 팝업의 일반 쇼핑몰 정보를 변환 및 적재하는 클래스.

    - **Extractor**: `AccountNormal`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: sabangnet_account`
    """

    extractor = "AccountNormal"
    tables = {"table": "sabangnet_account"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data",
        fields = ["shmaId", "acntRegsSrno"],
    )
