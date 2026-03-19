from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, HtmlTransformer, DuckDBTransformer

from typing import TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal
    from linkmerce.common.transform import JsonObject
    from bs4 import BeautifulSoup, Tag

Props = TypeVar("Props", bound=dict)



###################################################################
########################### Naver Search ##########################
###################################################################

class SearchSectionParser(HtmlTransformer):
    """네이버 검색 결과의 각 섹션을 파싱하여 하위 요소를 `list[dict]`로 정리하고 각 섹션을 `list`로 묶어서 반환하는 클래스.

    다음 섹션을 파싱한다:
    - 파워링크
    - 연관검색어
    - 네이버 가격비교
    - 네이버플러스 스토어
    - 스마트 블록 (브랜드 콘텐츠 등)
    - 웹문서 (블로그, 카페, 뉴스, 지식iN, 리뷰 등)
    - 이미지
    - 동영상/클립
    - AI 추천
    - AI 브리핑
    - 신제품소개
    """

    def transform(self, obj: str, mobile: bool = True, sep: str = '\n', **kwargs) -> list[list[dict]]:
        from linkmerce.utils.parse import select
        from bs4 import BeautifulSoup
        source = BeautifulSoup(obj, "html.parser")

        results = list()
        parent = "div#container > div#ct" if mobile else "div#container > div#content > div#main_pack"
        for section in self.select_sections(source, parent):
            id = section.get("id") or str()
            if (section.name == "section") and (links := PowerLink().transform(source, sep=sep)):
                results.append(links)
            elif id == "_related_keywords": # 연관검색어
                results.append(RelatedKeywords().transform(source, sep))
            elif id.startswith("shp_") and id.endswith("_root"): # 네이버 가격비교
                results.append(Shopping().transform(obj, sep))
            elif id.startswith("shs_") and id.endswith("_root"): # 네이버플러스 스토어
                results.append(NewShopping().transform(obj))
            elif id.startswith("fdr-") and (data := self.get_props_by_id(obj, id, sep)): # 스마트 블록, 웹문서 등
                results.append(data)
            elif "new_product" in (select(section, "section > :class():") or list()): # 신제품소개
                results.append([dict(section="신제품소개")]) # pass
            elif (blocks := self.select_nested_blocks(section)): # 스마트 블록, 웹문서 등
                results.append([prop for e in blocks for prop in self.get_props_by_id(obj, e.get("id"), sep)])
            else: # 미확인 섹션
                heading = select(section, "h2 > :text():") or select(section, "h3 > :text():")
                results.append([dict(section=heading)])
        return results

    @property
    def notin(self) -> list[str]:
        """섹션 목록 추출 과정에서 제외해야 할 CSS 선택자 패턴"""
        common = ["script", "#snb", "._scrollLog"]
        from_pc = [".api_sc_page_wrap", '[class*="feed_wrap"]', "._scrollLogEndline"]
        from_mobile = [".sp_page", '[class*="feed_more"]', '[data-slog-container="pag"]']
        shop_css = ['[id^="shp_"][id$="_css"]', '[id^="shs_"][id$="_css"]']
        return common + from_pc + from_mobile + shop_css

    def select_sections(self, source: BeautifulSoup, parent: str, depth: int = 1) -> list[Tag]:
        """파싱 대상 섹션과 관련 없는 CSS 선택자 패턴(`notin`)을 제외한 영역 내 섹션 목록을 반환한다.   
        `<link href=".../index.css">` 태그의 경우 자식 요소 중에서 섹션 목록을 추가로 추출한다."""
        from bs4 import Tag
        sections = list()
        for e in source.select(" > ".join([parent, ''.join([f":not({selector})" for selector in self.notin])])):
            if not isinstance(e, Tag): continue
            elif e.name == "link":
                if (depth == 1) and e.get("href", str()).endswith("/index.css"):
                    sections += self.select_sections(e, parent='&', depth=(depth+1))
            else: sections.append(e)
        return sections

    def select_nested_blocks(self, div: Tag) -> list[Tag]:
        "`fdr-` 접두사 `id`를 가진 영역은 하위 블록을 재귀적으로 탐색해 반환한다."
        from bs4 import Tag
        blocks, children = list(), list()
        for e in div.children:
            if isinstance(e, Tag):
                (blocks if (e.get("id") or str()).startswith("fdr-") else children).append(e)

        if blocks:
            return blocks
        for child in children:
            blocks += self.select_nested_blocks(child)
        return blocks

    def get_props_by_id(self, response: str, id: str, sep: str = '\n') -> list[dict]:
        """인라인 스크립트에서 `id`에 매칭되는 JSON 데이터를 추출해 `fender_renderer` 메서드로 변환한다."""
        from linkmerce.utils.regex import regexp_extract
        import json
        forward = r"document\.getElementById\(\"{}\"\),\s*".format(id)
        backward = r",\s*\{\s*onRendered:\s*function\(detail\)"
        body = regexp_extract(forward + r"(\{.*\})" + backward, response)
        try:
            data = json.loads(body)
            return self.fender_renderer(data, sep)
        except (json.JSONDecodeError, KeyError):
            return list()

    def fender_renderer(self, data: JsonObject, sep: str = '\n') -> list[dict]:
        """`ssuidExtra` 식별자로 섹션을 특정하고 해당하는 `Transformer`를 선택해 하위 블록을 파싱한다."""
        ssuid = str(data["meta"]["ssuidExtra"]).replace("fender_renderer-", str())
        if ssuid == "intentblock": # 스마트 블록 (브랜드 콘텐츠)
            return IntentBlock().transform(data, sep)
        elif ssuid == "web": # 웹문서 (블로그, 카페, 뉴스, 지식iN 등)
            return Web().transform(data, sep)
        elif ssuid == "image": # 이미지
            return Image().transform(data)
        elif ssuid == "video": # 동영상/클립
            return Video().transform(data)
        elif ssuid == "review": # 리뷰
            return Review().transform(data)
        elif ssuid == "qra": # AI 추천
            return RelatedQuery().transform(data)
        elif ssuid == "ai_briefing": # AI 브리핑
            return AiBriefing().transform(data)
        else: # 미확인 섹션
            return list()


class PowerLink(HtmlTransformer):
    """네이버 파워링크 광고 영역을 파싱하는 클래스."""

    def transform(self, section: BeautifulSoup, sep: str = '\n') -> list[dict]:
        """네이버 파워링크 광고 영역의 각 광고 구좌를 추출해 반환한다."""
        return [self.parse(li, sep) for li in section.select("ul#power_link_body > li")]

    def parse(self, li: Tag, sep: str = '\n') -> dict:
        """광고 구좌에서 분석용 항목들을 추출 또는 변환해 반환한다."""
        from linkmerce.utils.parse import select
        return {
            "section": "파워링크",
            "subject": None,
            "title": select(li, "div.tit_area > :text():"),
            "description": select(li, "a.desc > :text():"),
            "url": select(li, "a.txt_link > :attr(href):"),
            "profile_name": select(li, "span.site > :text():"), # site_name
            "image_url": (img.get("src") if (img := li.select_one("img:not(.icon_favicon)")) else None),
            # "image_urls": sep.join([src for image in li.select("img:not(.icon_favicon)") if (src := image.get("src"))]),
            # "keywords": sep.join([span.get_text(strip=True) for span in select(li, "span.keyword_item")]),
        }


class RelatedKeywords(HtmlTransformer):
    """네이버 연관검색어 영역을 파싱하는 클래스."""

    def transform(self, section: BeautifulSoup, sep: str = '\n') -> list[dict]:
        """네이버 연관검색어 영역의 키워드를 추출해 반환한다."""
        return [{
            "section": "연관검색어",
            "subject": None,
            "keywords": sep.join(self.parse(section)) or None,
        }]

    def parse(self, section: BeautifulSoup) -> list[str]:
        """키워드 텍스트를 리스트로 추출해 반환한다."""
        return [a.get_text(strip=True) for a in section.select("div.keyword > a")]


class Shopping(HtmlTransformer):
    """네이버 가격비교 영역을 파싱하는 클래스."""

    key: Literal["shopping", "nstore"] = "shopping"

    def transform(self, response: str, sep: str = '\n') -> list[dict]:
        """네이버 가격비교 영역에서 상품 또는 필터 목록을 추출해 반환한다."""
        props = self.get_props(response)
        if props:
            return (self.parse_filter_set(props, sep) + self.parse_products(props))
        else:
            return [dict(section="네이버 가격비교")]

    def get_props(self, response: str) -> dict:
        """인라인 스크립트에서 `key`에 해당하는 `_INITIAL_STATE` JSON을 추출해 `initProps` 데이터를 반환한다."""
        from linkmerce.utils.regex import regexp_extract
        import json
        import re
        object = r'naver\.search\.ext\.newshopping\["{}"\]'.format(self.key)
        raw_json = regexp_extract(object+r'\._INITIAL_STATE=(\{.*\})\n', response)
        raw_json = re.sub(r'new Date\(("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z")\)', r"\g<1>", raw_json)
        raw_json = re.sub(r":\s*undefined", ":null", raw_json)
        try:
            return json.loads(raw_json)["initProps"]
        except (json.JSONDecodeError, KeyError):
            return dict()

    def parse_products(self, props: dict) -> list[str]:
        """`initProps` 데이터에서 모든 페이지의 상품 목록을 파싱한다."""
        products = list()
        for page in (props.get("pagedSlot") or list()):
            if isinstance(page, dict):
                for slot in (page.get("slots") or list()):
                    if isinstance(slot, dict) and isinstance(slot.get("data"), dict):
                        products.append(self.parse_product(slot["data"], page.get("page")))
        return products

    def parse_product(self, data: dict, page: int | None = None) -> dict:
        """상품 원본 데이터에서 분석용 항목들을 추출 또는 변환해 반환한다."""
        from linkmerce.utils.nested import hier_get
        ad_desc_keys = ["adPromotionDescription", "adPromotionLongDescription"]
        return {
            "section": "네이버 가격비교",
            "subject": (self.source_type.get(data.get("sourceType")) or data.get("sourceType")),
            "page": page,
            "id": data.get("nvMid"),
            "product_id": (data.get("channelProductId") or None),
            "product_type": (self.card_type.get(data.get("cardType")) or data.get("cardType")),
            "title": (data.get("productNameOrg") or data.get("productName") or None),
            "url": (hier_get(data, "productUrl.pcUrl") or hier_get(data, "productClickUrl.pcUrl") or None),
            "image_url": (hier_get(data, "images.0.imageUrl") or None),
            "category_id": int(id) if (id := str(data.get("leafCategoryId"))).isdigit() else None,
            "channel_seq": data.get("merchantNo"),
            "mall_seq": (int(seq) if (seq := str(data.get("mallSeq"))).isdigit() and (seq != '0') else None),
            "mall_name": (data.get("mallName") or None),
            "mall_url": (hier_get(data, "mallUrl.pcUrl") or None),
            "delivery_type": (self.delivery_type.get(data.get("fastDeliveryType")) or data.get("fastDeliveryType")),
            "price": data.get("salePrice"),
            "sales_price": data.get("discountedSalePrice"),
            "review_score": data.get("averageReviewScore"),
            "review_count": data.get("totalReviewCount"),
            "purchase_count": data.get("purchaseCount"),
            "keep_count": data.get("keepCount"),
            "mall_count": data.get("mallCount"),
            **({
                "ad_id": good_id,
                "description": (" / ".join(data[key] for key in ad_desc_keys if data.get(key)) or None),
            } if (good_id := str(data.get("gdid"))).startswith("nad-") else dict()),
        }

    def parse_filter_set(self, props: dict, sep: str = '\n') -> list[dict]:
        """브랜드, 키워드추천 등 필터 영역(상품 영역 상단)이 있을 경우 각 필터 영역의 버튼 텍스트 목록을 파싱한다."""
        filter_set = list()
        for set_ in (props.get("filterSet") or list()):
            if isinstance(set_, dict):
                values = [value for value in (set_.get("values") or list()) if isinstance(value, dict)]
                filter_set.append({
                    "section": "네이버 가격비교",
                    "subject": "필터",
                    "title": set_.get("name"),
                    "ids": sep.join([(value.get("id") or str()) for value in values]),
                    "names": sep.join([(value.get("name") or str()) for value in values]),
                })
        return filter_set

    @property
    def source_type(self) -> dict[str,str]:
        return {
            "AD": "광고 상품",
            "SUPER_POINT": "광고+ 상품",
            "SAS": "일반 상품",
        }

    @property
    def card_type(self) -> dict[str,str]:
        return {
            "AD_CARD": "광고 상품",
            "ORGANIC_CARD": "일반 상품",
            "CATALOG_CARD": "가격비교 상품",
            "SUPER_POINT_CARD": "슈퍼적립 상품",
        }

    @property
    def delivery_type(self) -> dict[str,str]:
        return {
            "NONE": "일반배송",
            "TODAY_DELIVERY": "오늘출발",
            "ARRIVAL_GUARANTEE": "도착보장",
            "OUTSIDE_MALL_FULFILLMENT": "빠른배송",
        }


class NewShopping(Shopping):
    """네이버플러스 스토어 영역을 파싱하는 클래스."""

    key: Literal["shopping", "nstore"] = "nstore"

    def transform(self, response: str) -> list[dict]:
        """네이버플러스 스토어 영역에서 상품 목록을 추출해 반환한다."""
        props = self.get_props(response)
        if props:
            return self.parse_products(props)
        else:
            return [dict(section="네이버플러스 스토어")]

    def parse_products(self, props: dict) -> list[str]:
        """`initProps` 데이터에서 상품 목록을 파싱한다."""
        products = list()
        for product in (props.get("products") or list()):
            if isinstance(product, dict):
                products.append(self.parse_product(product))
        return products

    def parse_product(self, data: dict) -> dict:
        """상품 원본 데이터에서 분석용 항목들을 추출 또는 변환해 반환한다."""
        product = super().parse_product(data)
        for exclude in ["page", "category_id", "purchase_count", "keep_count", "mall_count", "description"]:
            product.pop(exclude, None)
        product["section"] = "네이버플러스 스토어"
        return product


class _PropsTransformer(JsonTransformer):
    """`fdr-` 접두사 `id`를 가진 영역에서 공통적으로 확인되는 props JSON 구조를 파싱하는 클래스."""
    dtype = dict
    scope = "body.props"

    def get_props(self, data: dict[str,dict]) -> Props:
        """`body.props` 경로에서 props JSON 데이터를 추출한다."""
        return data["body"]["props"]

    def split_layout(self, props: Props) -> tuple[Props, Props]:
        """첫 번째 자식이 `header`인 경우 `(header, layout)`으로 분리하고, 아니면 `(None, props)`를 반환한다."""
        template_id = str(props["children"][0]["templateId"])
        if (template_id == "header") or template_id.endswith("Header"):
            header, layout = props["children"][:2]
            return header, layout["props"]
        else:
            return None, props

    def clean_html(self, __object: Any, strip: bool = True) -> str | None:
        """HTML 태그를 제거하고 텍스트를 반환한다."""
        from linkmerce.utils.parse import clean_html as clean
        return clean(__object, strip=strip) if isinstance(__object, str) else None

    def coalesce_text(self, props: Props, keys: list[str], strip: bool = True) -> str | None:
        """`props` 데이터에서 첫 번째 비어 있지 않은 키의 HTML 텍스트를 반환한다."""
        for key in keys:
            if props.get(key):
                return self.clean_html(props[key], strip=strip)


class _ContentsPropsTransformer(_PropsTransformer):
    """스마트 블록/웹문서를 파싱하기 위한 공통 메서드를 가진 기반 클래스."""

    def parse_subject(self, header: dict) -> str:
        """제목 텍스트를 정규화한다."""
        title = header["props"]["title"] or str()
        if title.endswith("' 관련 브랜드 콘텐츠"):
            return "브랜드 콘텐츠"
        elif title.endswith("' 인기글"):
            return "인기글"
        else:
            return title

    def parse_items(self, props: Props) -> list[Props]:
        """`props` 데이터에서 `Item` 또는 `ItemMo`로 끝나는 ID를 가진 하위 블럭을 재귀적으로 탐색해 리스트로 반환한다."""
        items = list()
        for child in props["children"]:
            template_id = str(child["templateId"])
            if template_id == "healthItem":
                pass
            elif template_id.endswith("Item") or template_id.endswith("ItemMo"):
                items.append(child)
                continue
            from linkmerce.utils.nested import hier_get
            if hier_get(child, "props.children"):
                items += self.parse_items(child["props"])
        return items


class IntentBlock(_ContentsPropsTransformer):
    """네이버 스마트 블록 영역에 해당하는 props JSON 데이터를 파싱하는 클래스."""

    # @try_except
    def transform(self, data: dict[str,dict], sep: str = '\n', **kwargs) -> list[dict]:
        """네이버 스마트 블록 영역의 제목과 하위 컨텐츠를 추출해 반환한다."""
        props = self.get_props(data)
        header, layout = self.split_layout(props)
        subject = self.parse_subject(header) if header else None
        contents = list()
        for item in self.parse_items(layout):
            id = item["templateId"]
            if id == "accordionItem": # 아코디언 형태의 영역
                contents += [
                    self.parse(child["props"], subject, mobile=False, sep=sep)
                    for child in item["props"]["children"]]
            else:
                contents.append(self.parse(item["props"], subject, mobile=(id == "ugcItemMo"), sep=sep))
        return contents

    def parse(self, props: Props, subject: str, mobile: bool = False, sep: str = '\n') -> dict:
        """하위 컨텐츠의 `props` 데이터에서 분석용 항목들을 추출 또는 변환해 반환한다."""
        from linkmerce.utils.nested import hier_get, coalesce
        from linkmerce.utils.regex import regexp_extract
        article = (props.get("article") or dict()) if mobile else props
        profile = props.get("profile" if mobile else "sourceProfile") or dict()
        return {
            "section": "스마트블록",
            "subject": subject,
            "title": self.clean_html(article.get("title")),
            "description": self.clean_html(article.get("content")),
            "url": article.get("titleHref"),
            **({"ad_id": regexp_extract(r"(nad-a001-03-\d+)", hier_get(article, "clickLog.title.i") or str())}
                if subject == "브랜드 콘텐츠" else dict()),
            "image_count": article.get("imageCount") or 0,
            "image_url": hier_get(article, ("thumbObject.src" if mobile else "images.0.imageSrc")),
            # "image_urls": sep.join([image["imageSrc"] for image in (article.get("images") or list())]),
            "profile_name": coalesce(profile, ["text", "title"] if mobile else ["title", "text"]),
            "profile_url": coalesce(profile, ["href", "titleHref"] if mobile else ["titleHref", "href"]),
            "created_date": coalesce(profile, ["subText", "createdDate"] if mobile else ["createdDate", "subText"]),
        }


class Web(_ContentsPropsTransformer):
    """네이버 웹문서 영역에 해당하는 props JSON 데이터를 파싱하는 클래스."""

    # @try_except
    def transform(self, data: dict[str,dict], sep: str = '\n', **kwargs) -> list[dict]:
        """네이버 웹문서 영역의 구조를 분석하고 하위 문서를 추출해 반환한다."""
        props = self.get_props(data)
        header, layout = self.split_layout(props)
        subject = self.parse_subject(header) if header else "외부 사이트"
        return [self.parse(item["props"], subject, sep) for item in self.parse_items(layout)]

    def parse(self, props: Props, subject: str, sep: str = '\n') -> dict:
        """하위 문서의 `props` 데이터에서 분석용 항목들을 추출 또는 변환해 반환한다."""
        from linkmerce.utils.nested import hier_get
        profile = props.get("profile") or dict()
        # more = hier_get(props, "aggregation.contents.0") or dict()
        return {
            "section": "웹문서",
            "subject": subject,
            "title": self.clean_html(props.get("title")),
            "description": self.clean_html(props.get("bodyText")),
            "url": props.get("href"),
            "image_url": hier_get(props, "images.0.imageSrc"),
            # "image_urls": sep.join([image["imageSrc"] for image in (props.get("images") or list())]),
            "profile_name": profile.get("title"),
            "profile_url": profile.get("href"),
            # "profile_icon_url": profile.get("favicon"),
            # "more_title": more.get("title"),
            # "more_": more.get("bodyText"),
            # "more_url": more.get("href"),
        }


class Review(Web):
    """네이버 리뷰 영역에 해당하는 props JSON 데이터를 파싱하는 클래스."""

    # @try_except
    def transform(self, data: dict[str,dict], **kwargs) -> list[dict]:
        """네이버 리뷰 영역의 구조를 분석하고 하위 문서를 추출해 반환한다."""
        props = self.get_props(data)
        header, layout = self.split_layout(props)
        subject = self.parse_subject(header) if header else "리뷰"
        return [self.parse(item["props"], subject) for item in self.parse_items(layout)]

    def parse(self, props: Props, subject: str) -> dict:
        """하위 문서의 `props` 데이터에서 분석용 항목들을 추출 또는 변환해 반환한다."""
        profile = props.get("sourceProfile") or dict()
        return {
            "section": "웹문서",
            "subject": subject,
            "title": self.clean_html(props.get("title")),
            "description": self.clean_html(props.get("content")),
            "url": props.get("titleHref"),
            "profile_name": profile.get("title"),
            "profile_url": profile.get("titleHref"),
            # "profile_image_url": profile.get("imageSrc"),
            "created_date": profile.get("createdDate"),
        }


class Image(_PropsTransformer):
    """네이버 이미지 영역에 해당하는 props JSON 데이터를 파싱하는 클래스."""

    # @try_except
    def transform(self, data: dict[str,dict], **kwargs) -> list[dict]:
        """네이버 이미지 영역의 구조를 분석하고 하위 문서를 추출해 반환한다."""
        props = self.get_props(data)
        header, layout = self.split_layout(props)
        return [self.parse(image["props"]) for image in layout["children"]]

    def parse(self, props: Props) -> dict:
        """하위 이미지의 `props` 데이터에서 분석용 항목들을 추출 또는 변환해 반환한다."""
        # from linkmerce.utils.map import hier_get
        return {
            "section": "이미지",
            "subject": None,
            "title": props.get("text"),
            # "title_icon_url": hier_get(props, "icon.src"),
            "params": props.get("link"),
        }


class Video(_PropsTransformer):
    """네이버 동영상 영역에 해당하는 props JSON 데이터를 파싱하는 클래스."""

    # @try_except
    def transform(self, data: dict[str,dict], **kwargs) -> list[dict]:
        """네이버 동영상 영역의 구조를 분석하고 하위 문서를 추출해 반환한다."""
        from linkmerce.utils.nested import hier_get
        props = self.get_props(data)
        header, layout = self.split_layout(props)
        subject = hier_get(header, "props.title") if header else None
        return [self.parse(video["props"], subject) for video in layout["children"]]

    def parse(self, props: Props, subject: str) -> dict:
        """하위 동영상의 `props` 데이터에서 분석용 항목들을 추출 또는 변환해 반환한다."""
        return {
            "section": "동영상",
            "subject": subject,
            "title": self.coalesce_text(props, ["html", "imageAlt"]),
            "description": self.coalesce_text(props, ["description", "descriptionHtml", "content"]),
            "url": props.get("href"),
            "image_url": (props.get("imageSrc") or props.get("thumbImageSrc")),
            "profile_name": (props.get("author") or props.get("source")),
            "profile_url": (props.get("authorHref") or props.get("profileHref")),
            # "profile_image_url": props.get("profileImageSrc"),
            "created_date": props.get("createdAt"),
        }


class RelatedQuery(_PropsTransformer):
    """네이버 AI 추천 영역에 해당하는 props JSON 데이터를 파싱하는 클래스."""

    # @try_except
    def transform(self, data: dict[str,dict], **kwargs) -> list[dict]:
        """네이버 AI 추천 영역의 유형을 파악하고 추천 문서 목록과 연결된 API URL을 추출해 반환한다."""
        props = self.get_props(data)
        query_type = str(data["meta"]["xQuerySource"]).rsplit('_', 1)[0]
        return [{
            "section": "AI 추천",
            "subject": ("함께 보면 좋은" if query_type == "sbs" else "함께 많이 찾는"),
            "query_type": query_type,
            "url": self.parse(props, query_type),
        }]

    def parse(self, props: Props, query_type: Literal["nd", "sbs"]) -> str | None:
        """네이버 AI 추천 문서 목록은 API URL에 추가 요청해야 확인할 수 있다. 해당 URL을 반환한다."""
        if ("apiURLs" in props) and isinstance(props["apiURLs"], dict):
            return props["apiURLs"].get(query_type)
        else:
            return props.get("apiURL")


class AiBriefing(_PropsTransformer):
    """네이버 AI 브리핑 영역에 해당하는 props JSON 데이터를 파싱하는 클래스."""

    # @try_except
    def transform(self, data: dict[str,dict], **kwargs) -> list[dict]:
        """네이버 AI 브리핑 영역의 각 부분을 추출해 반환한다. (관련 자료 부분만 추출한다.)"""
        props = self.get_props(data)
        return self.parse_sources(props)

    def parse_media(self, props: Props) -> list[dict]:
        def get_type(type: str) -> str:
            return {"image":"이미지", "video":"동영상"}.get(type, type)
        return [{
            "section": "AI 브리핑",
            "subject": "관련 영상",
            "title": media.get("title"),
            "platform": media.get("platform"),
            "url": media.get("url"),
            "type": get_type(media.get("type")),
            "image_url": media.get("thumbnailUrl"),
        } for media in (props.get("multimedia") or list()) if isinstance(media, dict)]

    def parse_summary(self, props: Props) -> list[dict]:
        from linkmerce.utils.nested import hier_get
        return [{
            "section": "AI 브리핑",
            "subject": "요약",
            "summary": hier_get(props, "summary.markdown"),
        }]

    def parse_sources(self, props: Props) -> list[dict]:
        return [{
            "section": "AI 브리핑",
            "subject": "관련 자료",
            "title": source.get("title"),
            "description": source.get("content"),
            "platform": source.get("platform"),
            "url": source.get("url"),
        } for source in (props.get("sources") or list()) if isinstance(source, dict)]

    def parse_questions(self, props: Props) -> list[dict]:
        return [{
            "section": "AI 브리핑",
            "subject": "관련 질문",
            "question": question["title"],
        } for question in (props.get("relatedQuestions") or list())]

    def parse_info(self, props: Props) -> list[dict]:
        from linkmerce.utils.nested import hier_get
        return [{
            "section": "AI 브리핑",
            "subject": "관련 정보",
            "title": info.get("title"),
            "text": subinfo.get("text"),
        } for info in (hier_get(props, "summaryInfo.info") or list()) if isinstance(info, dict)
            for subinfo in (info.get("subInfos") or list()) if isinstance(subinfo, dict)]


class Search(DuckDBTransformer):
    """네이버 검색 결과의 각 섹션별 하위 블럭을 직렬화 또는 요약하여 각각의 테이블에 적재하는 클래스.

    테이블 키 | 테이블명 | 설명
    - `sections` | `naver_search_sections` | 네이버 검색 결과의 각 섹션 목록
    - `summary` | `naver_search_summary` | 네이버 검색 결과 요약"""

    tables = {"sections": "naver_search_sections", "summary": "naver_search_summary"}
    parser = SearchSectionParser

    def transform(self, obj: str, query: str, mobile: bool = True, sep: str = '\n', **kwargs) -> list:
        """네이버 검색 결과의 각 섹션별 하위 블럭을 직렬화 또는 요약하여 각각의 테이블에 적재한다."""
        sections = SearchSectionParser().transform(obj, mobile, sep)
        if sections:
            results = self.insert_serialized_sections(query, sections)
            summary = self.summarize_sections(query, sections, sep)
            return results + self.bulk_insert(summary, render={"summary": self.tables["summary"]})
        else:
            return list()

    def insert_serialized_sections(self, query: str, sections: list[list[dict]]) -> list:
        """네이버 검색 결과를 JSON으로 직렬화하여 `naver_search_sections` 테이블에 삽입한다."""
        import json
        json_str = json.dumps(sections, ensure_ascii=False, separators=(',', ':'))
        table = self.tables["sections"]
        return self.conn.execute(f"INSERT INTO {table} (query, sections) VALUES (?, ?)", params=(query, json_str))

    def summarize_sections(self, query: str, sections: list[list[dict]], sep: str = '\n') -> list[dict]:
        """각 섹션별 하위 블럭을 요약하여 리스트로 반환한다."""
        from collections import Counter
        from linkmerce.utils.nested import hier_get
        summary = list()
        for seq, section in enumerate(sections, start=1):
            heading = hier_get(section, "0.section")
            subjects = [item["subject"] for item in section if item.get("subject") and (item.get("page", 1) == 1)]
            if not heading:
                continue
            elif heading == "연관검색어":
                keyword_count = len((section[0].get("keywords") or str()).split(sep))
                summary.append(dict(query=query, seq=seq, section=heading, subject=str(), item_count=keyword_count))
            elif (len(section) == 1) and (len(section[0]) == 1):
                summary.append(dict(query=query, seq=seq, section=heading, subject=str(), item_count=0))
            elif (not subjects) or (heading == "웹문서"):
                summary.append(dict(query=query, seq=seq, section=heading, subject=str(), item_count=len(section)))
            else:
                for subject, count in Counter(subjects).items():
                    summary.append(dict(query=query, seq=seq, section=heading, subject=subject, item_count=count))
        return summary


###################################################################
######################## Mobile Tab Search ########################
###################################################################

class CafeParser(HtmlTransformer):
    """네이버 모바일 카페 탭 검색 결과 HTML 소스코드를 파싱하는 클래스."""

    scope = "div.view_wrap:all"
    fields = {
        "url": "a.title_link > :attr(href):",
        "rank": None, # $metadata.seq
        "cafe_url": None, # get_ids_from_url(url)
        "article_id": None, # get_ids_from_url(url)
        "onclick": "a.title_link > :attr(onclick):", # pop()
        "ad_id": None, # get_ad_id_from_attr(onclick)
        "cafe_name": "div.user_info > a.name > :text():",
        "title": "a.title_link > :text():",
        "description": "div.dsc_area > :text():",
        "image_url": "a.thumb_link > img > :attr(src):",
        "article_url": None, # make_article_url(url, query)
        "replies": "div.flick_bx:all", # select_replies(replies)
        "write_date": "div.user_info > span.sub > :text():"
    }
    extends = {
        "rank": "$metadata.seq"
    }

    def extend_fields(self, item: dict, extends = None, **kwargs) -> dict:
        """필드를 추출한 후 파생 필드를 생성하거나 값을 변환한다."""
        extends = dict(zip(["cafe_url", "article_id"], self.get_ids_from_url(item["url"])))
        extends.update({
            "ad_id": self.get_ad_id_from_attr(item.pop("onclick")),
            "article_url": self.make_article_url(item["url"], kwargs["query"]),
            "replies": '\n'.join(self.select_replies(item["replies"])) or None,
        })
        return super().extend_fields(item, extends, **kwargs)

    def get_ids_from_url(self, url: str) -> tuple[str,str]:
        """카페 URL에서 카페 URL과 게시물 ID를 추출하여 튜플로 반환한다."""
        from linkmerce.utils.regex import regexp_groups
        return regexp_groups(r"/([^/]+)/(\d+)$", url.split('?')[0], indices=[0,1]) if url else (None, None)

    def get_ad_id_from_attr(self, onclick: str) -> str:
        """onclick 속성에서 소재 ID를 정규식으로 추출하여 반환한다."""
        from linkmerce.utils.regex import regexp_extract
        return regexp_extract(r"(nad-a\d+-\d+-\d+)", onclick) if onclick else None

    def make_article_url(self, url: str, query: str) -> str | None:
        """카페 URL과 검색어를 바탕으로 카페 게시물 API URL을 조립하여 반환한다."""
        cafe_url, article_id = self.get_ids_from_url(url)
        if (cafe_url is not None) and (article_id is not None):
            params = self.make_article_params(url, query)
            return f"https://article.cafe.naver.com/gw/v4/cafes/{cafe_url}/articles/{article_id}?{params}"

    def make_article_params(self, url: str, query: str) -> str:
        """카페 게시물 API 요청에 필요한 쿼리 파라미터 문자열을 생성하여 반환한다."""
        from urllib.parse import quote, urlencode
        from uuid import uuid4
        param_string = url.split('?')[1] if '?' in url else str()
        params = dict([kv.split('=', maxsplit=1) for kv in param_string.split('&')]) if param_string else dict()
        return urlencode(dict(params, **{
            "useCafeId": "false",
            "tc": "naver_search",
            "or": "{}search.naver.com".format("m." if url.startswith("https://m.") else str()),
            "query": quote(query),
            "buid": uuid4()
        }))

    def select_replies(self, flick_bx: list[Tag], prefix: str = "[RE] ") -> list[str]:
        """`div.flick_bx` 요소에서 답글 텍스트를 수집하여 리스트로 반환한다."""
        replies = list()
        for box in flick_bx:
            ico_reply = box.select_one("i.ico_reply")
            if ico_reply:
                ico_reply.decompose()
                replies.append(prefix + box.get_text(strip=True))
        return replies


class CafeTab(DuckDBTransformer):
    """네이버 모바일 카페 탭 검색 결과를 `naver_cafe` 테이블에 적재하는 클래스."""

    tables = {"table": "naver_cafe"}
    parser = CafeParser
    params = {"query": "$query"}


class CafeArticleParser(JsonTransformer):
    """네이버 카페 게시글 API 응답 데이터를 파싱하는 클래스."""
    dtype = dict
    scope = "result"
    fields = {
        ".": ["cafeId", "articleId", "tags", "comments.items"],
        "cafe": ["url", "name", "memberCount"],
        "article": [
            "menu.name", {"head": None}, "subject", {"writer": ["nick", "memberKey", "memberLevelName"]},
            "subject", "readCount", "commentCount", "writeDate", "contentHtml",
            # {"content": count_content(contentHtml)}
        ]
    }

    def extend_fields(self, item: dict, extends = None, **kwargs) -> dict:
        """필드를 추출한 후 파생 필드를 생성하거나 값을 변환한다."""
        extends = {
            "article.commenterCount": len({item["writer"]["memberKey"] for item in (item["comments"]["items"] or list())}),
            "tags": (", ".join(item["tags"]) if item["tags"] else None),
            "article.content": self.count_content(item["article"]["contentHtml"] or str()),
            "article.contentHtml": None,
        }
        return super().extend_fields(item, extends, **kwargs)

    def count_content(self, content: str) -> dict[str, int]:
        """카페 게시글 콘텐츠 HTML에서 단어 수와 이미지 수를 카운트하여 dict로 반환한다."""
        from bs4 import BeautifulSoup
        import re
        word_count, image_count = None, None
        try:
            source = BeautifulSoup(content.replace('\\\\', '\\'), "html.parser")
            for div in source.select("div.se-oglink"):
                div.decompose()
            word_count = len(re.sub(r"\s+", ' ', source.get_text()).strip())
            image_count = len(source.select("img.se-image-resource"))
        except:
            pass
        return {"wordCount": word_count, "imageCount": image_count}


class CafeArticle(DuckDBTransformer):
    """네이버 카페 게시글 API 응답 데이터를 `naver_cafe_article` 테이블에 적재하는 클래스."""

    tables = {"table": "naver_cafe_article"}
    parser = CafeArticleParser
