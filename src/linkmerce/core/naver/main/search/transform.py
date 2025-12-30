from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, HtmlTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal
    from linkmerce.common.transform import JsonObject
    from bs4 import BeautifulSoup, Tag


def parse_int(text: str) -> int | None:
    if text is None:
        return None
    import re
    groups = re.findall(r"\d{1,3}(?:,\d{3})+", text)
    return int(str(groups[0]).replace(',', '')) if groups else None


###################################################################
########################### Naver Search ##########################
###################################################################

class SearchSection(HtmlTransformer):

    def transform(self, obj: str, mobile: bool = True, sep: str = '\n', **kwargs) -> list[list[dict]]:
        from bs4 import BeautifulSoup
        source = BeautifulSoup(obj, "html.parser")

        results = list()
        parent = "div#container > div#ct" if mobile else "div#container > div#content > div#main_pack"
        for section in self.select_sections(source, parent):
            id = section.get("id") or str()
            if (section.name == "section") and (links := PowerLink().transform(source, sep=sep)):
                results.append(links)
            elif id == "_related_keywords":
                results.append(RelatedKeywords().transform(source, sep))
            elif id.startswith("shp_") and id.endswith("_root"):
                results.append(Shopping().transform(obj, sep))
            elif id.startswith("shs_") and id.endswith("_root"):
                results.append(NewShopping().transform(obj))
            elif id.startswith("fdr-") and (data := self.get_props_by_id(obj, id, sep)):
                results.append(data)
            elif "new_product" in (self.select(section, "section > :class():") or list()):
                results.append([dict(section="신제품소개")]) # pass
            elif (blocks := self.select_nested_blocks(section)):
                results.append([prop for e in blocks for prop in self.get_props_by_id(obj, e.get("id"), sep)])
            else:
                heading = self.select(section, "h2 > :text():") or self.select(section, "h3 > :text():")
                results.append([dict(section=heading)])
        return results

    def select_sections(self, source: BeautifulSoup, parent: str) -> list[Tag]:
        from bs4 import Tag
        sections = list()
        for e in source.select(" > ".join([parent, ''.join([f":not({selector})" for selector in self.notin])])):
            if not isinstance(e, Tag): continue
            elif e.name == "link":
                if e.get("href", str()).endswith("/index.css"):
                    sections += self.select_sections(e, parent='&')
            else: sections.append(e)
        return sections

    @property
    def notin(self) -> list[str]:
        common = ["script", "#snb", "._scrollLog"]
        from_pc = [".api_sc_page_wrap", '[class*="feed_wrap"]', "._scrollLogEndline"]
        from_mobile = [".sp_page", '[class*="feed_more"]', '[data-slog-container="pag"]']
        from_shop_div = ['[id^="shp_"][id$="_css"]', '[id^="shs_"][id$="_css"]']
        return common + from_pc + from_mobile + from_shop_div

    def select_nested_blocks(self, div: Tag) -> list[Tag]:
        from bs4 import Tag
        blocks, next_divs = list(), list()
        for e in div.children:
            if isinstance(e, Tag):
                (blocks if (e.get("id") or str()).startswith("fdr-") else next_divs).append(e)

        if blocks:
            return blocks
        for next_div in next_divs:
            blocks += self.select_nested_blocks(next_div)
        return blocks

    def get_props_by_id(self, response: str, id: str, sep: str = '\n') -> list[dict]:
        from linkmerce.utils.regex import regexp_extract
        import json
        forward = r"document\.getElementById\(\"{}\"\),\s*".format(id)
        backward = r",\s*\{\s*onRendered:\s*function\(detail\)"
        body = regexp_extract(forward + r"(\{.*\})" + backward, response)
        try:
            data = json.loads(body)
            return self.fender_renderer(data, sep)
        except json.JSONDecodeError:
            return list()

    def fender_renderer(self, data: JsonObject, sep: str = '\n') -> list[dict]:
        ssuid = str(data["meta"]["ssuidExtra"]).replace("fender_renderer-", str())
        if ssuid == "intentblock":
            return IntentBlock().transform(data, sep)
        elif ssuid == "web":
            return Web().transform(data, sep)
        elif ssuid == "image":
            return Image().transform(data)
        elif ssuid == "video":
            return Video().transform(data)
        elif ssuid == "review":
            return Review().transform(data)
        elif ssuid == "qra":
            return RelatedQuery().transform(data)
        elif ssuid == "ai_briefing":
            return AiBriefing().transform(data)
        else:
            return list() # undefined


class PowerLink(HtmlTransformer):

    def transform(self, section: BeautifulSoup, sep: str = '\n') -> list[dict]:
        return [self.parse(li, sep) for li in section.select("ul#power_link_body > li")]

    def parse(self, li: Tag, sep: str = '\n') -> dict:
        return {
            "section": "파워링크",
            "subject": None,
            "title": self.select(li, "div.tit_area > :text():"),
            "description": self.select(li, "a.desc > :text():"),
            "url": self.select(li, "a.txt_link > :attr(href):"),
            "profile_name": self.select(li, "span.site > :text():"), # site_name
            "image_url": (img.get("src") if (img := li.select_one("img:not(.icon_favicon)")) else None),
            # "image_urls": sep.join([src for image in li.select("img:not(.icon_favicon)") if (src := image.get("src"))]),
            # "keywords": sep.join([span.get_text(strip=True) for span in self.select(li, "span.keyword_item")]),
        }


class RelatedKeywords(HtmlTransformer):

    def transform(self, section: BeautifulSoup, sep: str = '\n') -> list[dict]:
        return [{
            "section": "연관검색어",
            "subject": None,
            "keywords": sep.join(self.parse(section)) or None,
        }]

    def parse(self, section: BeautifulSoup) -> list[str]:
        return [a.get_text(strip=True) for a in section.select("div.keyword > a")]


class _ShoppingTransformer(HtmlTransformer):
    key: Literal["shopping","nstore"]

    def get_props(self, response: str) -> dict:
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


class Shopping(_ShoppingTransformer):
    key = "shopping"

    def transform(self, response: str, sep: str = '\n') -> list[dict]:
        props = self.get_props(response)
        if props:
            return (self.parse_filter_set(props, sep) + self.parse_products(props))
        else:
            return [dict(section="네이버 가격비교")]

    def parse_products(self, props: dict) -> list[str]:
        products = list()
        for page in (props.get("pagedSlot") or list()):
            if isinstance(page, dict):
                for slot in (page.get("slots") or list()):
                    if isinstance(slot, dict) and isinstance(slot.get("data"), dict):
                        products.append(self.parse_product(slot["data"], page.get("page")))
        return products

    def parse_product(self, data: dict, page: int | None = None) -> dict:
        from linkmerce.utils.map import hier_get
        ad_description = [data.get("adPromotionDescription"), data.get("adPromotionLongDescription")]
        return {
            "section": "네이버 가격비교",
            "subject": (self.card_type.get(data.get("cardType")) or data.get("cardType")),
            "page": page,
            "id": data.get("nvMid"),
            "product_id": (data.get("channelProductId") or None),
            "category_id": int(id) if (id := str(data.get("leafCategoryId"))).isdigit() else None,
            **({"ad_id": id} if (id := str(data.get("gdid"))).startswith("nad-") else dict()),
            "title": (data.get("productNameOrg") or data.get("productName") or None),
            "url": (hier_get(data, ["productUrl","pcUrl"]) or hier_get(data, ["productClickUrl","pcUrl"]) or None),
            "image_url": (hier_get(data, ["images",0,"imageUrl"]) or None),
            "channel_seq": data.get("merchantNo"),
            "mall_seq": (int(seq) if (seq := str(data.get("mallSeq"))).isdigit() and (seq != '0') else None),
            "mall_name": (data.get("mallName") or None),
            "mall_url": (hier_get(data, ["mallUrl","pcUrl"]) or None),
            "delivery_type": (self.delivery_type.get(data.get("fastDeliveryType")) or data.get("fastDeliveryType")),
            "price": data.get("salePrice"),
            "sales_price": data.get("discountedSalePrice"),
            "review_score": data.get("averageReviewScore"),
            "review_count": data.get("totalReviewCount"),
            "purchase_count": data.get("purchaseCount"),
            "keep_count": data.get("keepCount"),
            "mall_count": data.get("mallCount"),
            "description": (" / ".join(filter(None, ad_description)) or None),
        }

    def parse_filter_set(self, props: dict, sep: str = '\n') -> list[dict]:
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
    key = "nstore"

    def transform(self, response: str) -> list[dict]:
        props = self.get_props(response)
        if props:
            return self.parse_products(props)
        else:
            return [dict(section="네이버플러스 스토어")]

    def parse_products(self, props: dict) -> list[str]:
        products = list()
        for product in (props.get("products") or list()):
            if isinstance(product, dict):
                products.append(self.parse_product(product))
        return products

    def parse_product(self, data: dict) -> dict:
        product = super().parse_product(data)
        for exclude in ["page", "category_id", "purchase_count", "keep_count", "mall_count", "description"]:
            product.pop(exclude, None)
        product["section"] = "네이버플러스 스토어"
        return product


class _PropsTransformer(JsonTransformer):
    dtype = dict
    path = ["body","props"]

    def get_props(self, data: dict[str,dict]) -> dict[str,Any]:
        return data["body"]["props"]

    def clean_html(self, __object: Any, strip: bool = True) -> str | None:
        from linkmerce.utils.parse import clean_html as clean
        return clean(__object, strip=strip) if isinstance(__object, str) else None

    def coalesce_text(self, props: dict[str,Any], keys: list[str], strip: bool = True) -> str | None:
        for key in keys:
            if props.get(key):
                return self.clean_html(props[key], strip=strip)


class IntentBlock(_PropsTransformer):

    # @try_except
    def transform(self, data: dict[str,dict], sep: str = '\n', **kwargs) -> list[dict]:
        props = self.get_props(data)
        header, contents, footer = props["children"]
        subject = self.parse_subject(header)
        return [self.parse(content["props"], subject=subject, template_id=template_id, sep=sep)
                for content in contents["props"]["children"]
                    if (template_id := content["templateId"]) != "dividerLine"]

    def parse(self, props: dict[str,Any], subject: str, template_id: str, sep: str = '\n') -> dict:
        from linkmerce.utils.map import hier_get
        from linkmerce.utils.regex import regexp_extract
        mo = (template_id == "ugcItemMo")
        article = (props.get("article") or dict()) if mo else props
        profile = props.get("profile" if mo else "sourceProfile") or dict()
        return {
            "section": "스마트블록",
            "subject": subject,
            "title": self.clean_html(article.get("title")),
            "description": self.clean_html(article.get("content")),
            "url": article.get("titleHref"),
            **({"ad_id": regexp_extract(r"(nad-a001-03-\d+)", hier_get(article, ["clickLog","title","i"]) or str())}
                if subject == "브랜드 콘텐츠" else dict()),
            "image_count": article.get("imageCount") or 0,
            "image_url": hier_get(article, (["thumbObject","src"] if mo else ["images",0,"imageSrc"])),
            # "image_urls": sep.join([image["imageSrc"] for image in (article.get("images") or list())]),
            "profile_name": profile.get("text" if mo else "title"),
            "profile_url": profile.get("href" if mo else "titleHref"),
            # "profile_image_url": article.get("imageSrc"),
            "created_date": profile.get("subText" if mo else "createdDate"),
        }

    def parse_subject(self, header: dict) -> str:
        title = header["props"]["title"] or str()
        if title.endswith("' 관련 브랜드 콘텐츠"):
            return "브랜드 콘텐츠"
        elif title.endswith("' 인기글"):
            return "인기글"
        else:
            return title


class Web(_PropsTransformer):

    # @try_except
    def transform(self, data: dict[str,dict], sep: str = '\n', **kwargs) -> list[dict]:
        props = self.get_props(data)
        return [self.parse(web["props"]) for web in props["children"][0]["props"]["children"]]

    def parse(self, props: dict[str,Any], sep: str = '\n') -> dict:
        from linkmerce.utils.map import hier_get
        profile = props.get("profile") or dict()
        # more = hier_get(props, ["aggregation","contents",0]) or dict()
        return {
            "section": "웹문서",
            "subject": "외부 사이트",
            "title": self.clean_html(props.get("title")),
            "description": self.clean_html(props.get("bodyText")),
            "url": props.get("href"),
            "image_url": hier_get(props, ["images",0,"imageSrc"]),
            # "image_urls": sep.join([image["imageSrc"] for image in (props.get("images") or list())]),
            "profile_name": profile.get("title"),
            "profile_url": profile.get("href"),
            # "profile_icon_url": profile.get("favicon"),
            # "more_title": more.get("title"),
            # "more_": more.get("bodyText"),
            # "more_url": more.get("href"),
        }


class Image(_PropsTransformer):

    # @try_except
    def transform(self, data: dict[str,dict], **kwargs) -> list[dict]:
        props = self.get_props(data)
        header, images, footer = props["children"]
        return [self.parse(image["props"]) for image in images["props"]["children"][0]["props"]["children"]]

    def parse(self, props: dict[str,Any]) -> dict:
        # from linkmerce.utils.map import hier_get
        return {
            "section": "이미지",
            "subject": None,
            "title": props.get("text"),
            # "title_icon_url": hier_get(props, ["icon","src"]),
            "params": props.get("link"),
        }


class Video(_PropsTransformer):

    # @try_except
    def transform(self, data: dict[str,dict], **kwargs) -> list[dict]:
        props = self.get_props(data)
        if props["children"][0]["templateId"] == "header":
            from linkmerce.utils.map import hier_get
            header, videos, footer = props["children"]
            videos = videos["props"]["children"]
            subject = hier_get(header, ["props","title"])
        else:
            videos = props["children"]
            subject = None
        return [self.parse(video["props"], subject) for video in videos]

    def parse(self, props: dict[str,Any], subject: str) -> dict:
        return {
            "section": "동영상",
            "subject": subject,
            "title": self.coalesce_text(props, ["html","imageAlt"]),
            "description": self.coalesce_text(props, ["description","descriptionHtml","content"]),
            "url": props.get("href"),
            "image_url": (props.get("imageSrc") or props.get("thumbImageSrc")),
            "profile_name": (props.get("author") or props.get("source")),
            "profile_url": (props.get("authorHref") or props.get("profileHref")),
            # "profile_image_url": props.get("profileImageSrc"),
            "created_date": props.get("createdAt"),
        }


class Review(_PropsTransformer):

    # @try_except
    def transform(self, data: dict[str,dict], **kwargs) -> list[dict]:
        props = self.get_props(data)
        return [self.parse(review["props"]) for review in props["children"][0]["props"]["children"]]

    def parse(self, props: dict[str,Any]) -> dict:
        profile = props.get("sourceProfile") or dict()
        return {
            "section": "웹문서",
            "subject": "리뷰",
            "title": self.clean_html(props.get("title")),
            "description": self.clean_html(props.get("content")),
            "url": props.get("titleHref"),
            "profile_name": profile.get("title"),
            "profile_url": profile.get("titleHref"),
            # "profile_image_url": profile.get("imageSrc"),
            "created_date": profile.get("createdDate"),
        }


class RelatedQuery(_PropsTransformer):

    # @try_except
    def transform(self, data: dict[str,dict], **kwargs) -> list[dict]:
        props = self.get_props(data)
        query_type = str(data["meta"]["xQuerySource"]).rsplit('_', 1)[0]
        return [{
            "section": "AI 추천",
            "subject": ("함께 보면 좋은" if query_type == "sbs" else "함께 많이 찾는"),
            "query_type": query_type,
            "url": self.parse(props, query_type),
        }]

    def parse(self, props: dict[str,Any], query_type: Literal["nd","sbs"]) -> str | None:
        if ("apiURLs" in props) and isinstance(props["apiURLs"], dict):
            return props["apiURLs"].get(query_type)
        else:
            return props.get("apiURL")


class AiBriefing(_PropsTransformer):

    # @try_except
    def transform(self, data: dict[str,dict], **kwargs) -> list[dict]:
        props = self.get_props(data)
        return self.parse_sources(props)

    def parse_media(self, props: dict[str,Any]) -> list[dict]:
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

    def parse_summary(self, props: dict[str,Any]) -> list[dict]:
        from linkmerce.utils.map import hier_get
        return [{
            "section": "AI 브리핑",
            "subject": "요약",
            "summary": hier_get(props, ["summary","markdown"]),
        }]

    def parse_sources(self, props: dict[str,Any]) -> list[dict]:
        return [{
            "section": "AI 브리핑",
            "subject": "관련 자료",
            "title": source.get("title"),
            "description": source.get("content"),
            "platform": source.get("platform"),
            "url": source.get("url"),
        } for source in (props.get("sources") or list()) if isinstance(source, dict)]

    def parse_questions(self, props: dict[str,Any]) -> list[dict]:
        return [{
            "section": "AI 브리핑",
            "subject": "관련 질문",
            "question": question["title"],
        } for question in (props.get("relatedQuestions") or list())]

    def parse_info(self, props: dict[str,Any]) -> list[dict]:
        from linkmerce.utils.map import hier_get
        return [{
            "section": "AI 브리핑",
            "subject": "관련 정보",
            "title": info.get("title"),
            "text": subinfo.get("text"),
        } for info in (hier_get(props, ["summaryInfo","info"]) or list()) if isinstance(info, dict)
            for subinfo in (info.get("subInfos") or list()) if isinstance(subinfo, dict)]


class Search(DuckDBTransformer):
    queries = ["create_sections", "create_summary", "select_summary", "insert_summary"]

    def set_tables(self, tables: dict | None = None):
        base = dict(sections="naver_search_sections", summary="naver_search_summary")
        super().set_tables(dict(base, **(tables or dict())))

    def create_table(self, params: dict = dict(), **kwargs):
        super().create_table(key="create_sections", table=":sections:", params=params)
        super().create_table(key="create_summary", table=":summary:", params=params)

    def transform(self, obj: str, query: str, mobile: bool = True, sep: str = '\n', **kwargs):
        sections = SearchSection().transform(obj, mobile, sep)
        if query == "요가양말":
            from bs4 import BeautifulSoup
            with open("var/요가양말.html",'w',encoding="utf-8") as file:
                file.write(BeautifulSoup(obj, 'html.parser').prettify())
        if sections:
            self.insert_sections(query, sections)
            self.insert_into_table(self.summarize_sections(query, sections),
                key=f"insert_summary", table=f":summary:", values=f":select_summary:")

    def summarize_sections(self, query: str, sections: list[list[dict]]) -> list[dict]:
        from collections import Counter
        from linkmerce.utils.map import hier_get
        summary = list()
        for seq, section in enumerate(sections, start=1):
            heading = hier_get(section, [0,"section"])
            subjects = [item["subject"] for item in section if item.get("subject")]
            if not heading:
                continue
            elif (len(section) == 1) and (len(section[0]) == 1):
                summary.append(dict(query=query, seq=seq, section=heading, subject=str(), item_count=0))
            elif not subjects:
                summary.append(dict(query=query, seq=seq, section=heading, subject=str(), item_count=len(section)))
            else:
                for subject, count in Counter(subjects).items():
                    summary.append(dict(query=query, seq=seq, section=heading, subject=subject, item_count=count))
        return summary

    def insert_sections(self, query: str, sections: list[list[dict]]):
        import json
        json_str = json.dumps(sections, ensure_ascii=False, separators=(',', ':'))
        table = self.get_table(":sections:")
        return self.conn.conn.execute(f"INSERT INTO {table} (query, sections) VALUES (?, ?)", (query, json_str))


###################################################################
######################## Mobile Tab Search ########################
###################################################################

class CafeList(HtmlTransformer):
    selector = "div.view_wrap"

    def transform(self, obj: BeautifulSoup, query: str, **kwargs) -> list[dict]:
        results = list()
        for rank, div in enumerate(obj.select(self.selector), start=1):
            results.append(self.parse(div, query, rank))
        return results

    def parse(self, div: Tag, query: str, rank: int) -> dict:
        url = self.select(div, "a.title_link > :attr(href):")
        return {
            "query": query,
            "rank": rank,
            **dict(zip(["cafe_url","article_id"], self.get_ids_from_url(url))),
            "ad_id": self.get_ad_id_from_attr(self.select(div, "a.title_link > :attr(onclick):")),
            "cafe_name": self.select(div, "div.user_info > a.name > :text():"),
            "title": self.select(div, "a.title_link > :text():"),
            "description": self.select(div, "div.dsc_area > :text():"),
            "url": url,
            "image_url": self.select(div, "a.thumb_link > img > :attr(src):"),
            "next_url": (self.make_next_url(url, query) if url else None),
            "replies": '\n'.join(self.parse_replies(div)) or None,
            "write_date": self.select(div, "div.user_info > span.sub > :text():"),
        }

    def get_ids_from_url(self, url: str) -> tuple[str,str]:
        from linkmerce.utils.regex import regexp_groups
        return regexp_groups(r"/([^/]+)/(\d+)$", url.split('?')[0], indices=[0,1]) if url else (None, None)

    def get_ad_id_from_attr(self, onclick: str) -> str:
        from linkmerce.utils.regex import regexp_extract
        return regexp_extract(r"(nad-a\d+-\d+-\d+)", onclick) if onclick else None

    def make_next_url(self, url: str, query: str) -> str:
        cafe_url, article_id = self.get_ids_from_url(url)
        m_ = "m." if url.startswith("https://m.") else str()
        if (cafe_url is None) or (article_id is None):
            return None

        from urllib.parse import quote
        from uuid import uuid4
        params = (p+'&') if (p := (url.split('?')[1] if '?' in url else None)) else str()
        params = f"{params}useCafeId=false&tc=naver_search&or={m_}search.naver.com&query={quote(query)}&buid={uuid4()}"
        return f"https://article.cafe.naver.com/gw/v4/cafes/{cafe_url}/articles/{article_id}?{params}"

    def parse_replies(self, div: Tag, prefix: str = "[RE] ") -> list[str]:
        replies = list()
        for box in div.select("div.flick_bx"):
            ico_reply = box.select_one("i.ico_reply")
            if ico_reply:
                ico_reply.decompose()
                replies.append(prefix + box.get_text(strip=True))
        return replies


class CafeTab(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, query: str, **kwargs):
        articles = CafeList().transform(obj, query)
        if articles:
            self.insert_into_table(articles)


class CafeArticleJson(JsonTransformer):
    path = ["result"]

    def parse(self, obj: JsonObject, **kwargs) -> JsonObject:
        result = obj["result"]
        result["article"]["commenterCount"] = len({item["writer"]["memberKey"] for item in result["comments"]["items"]})
        result["tags"] = ", ".join(result["tags"]) or None
        result["content"] = self.parse_content(result["article"]["contentHtml"])
        result["article"]["contentHtml"] = None
        return result

    def parse_content(self, content: str) -> dict:
        from bs4 import BeautifulSoup
        import re

        source = BeautifulSoup(content.replace('\\\\', '\\'), "html.parser")
        for div in source.select("div.se-oglink"):
            div.decompose()
        return {
            "contentLength": len(re.sub(r"\s+", ' ', source.get_text()).strip()),
            "imageCount": len(source.select("img.se-image-resource")),
        }


class CafeArticle(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: JsonObject, **kwargs):
        if obj is not None:
            self.insert_into_table([CafeArticleJson().transform(obj)])
