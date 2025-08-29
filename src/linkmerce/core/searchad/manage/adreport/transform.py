from __future__ import annotations

from linkmerce.common.transform import Transformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable, Literal
    import datetime as dt


def _convert(type: Literal["STRING","FLOAT","INTEGER","DATE"]) -> Callable[[str], str | float | int | dt.date]:
    if type == "FLOAT":
        from linkmerce.utils.cast import safe_float
        return lambda x: safe_float(x)
    elif type == "INTEGER":
        from linkmerce.utils.cast import safe_int
        return lambda x: safe_int(x)
    elif type == "DATE":
        from linkmerce.utils.date import safe_strpdate
        return lambda x: safe_strpdate(x, format="%Y.%m.%d.")
    else:
        return lambda x: x


class AdvancedReport(Transformer):

    def transform(self, obj: str, convert_dtypes: bool = True, **kwargs) -> list[list[str]]:
        from io import StringIO
        import csv
        if obj:
            reader = csv.reader(StringIO(obj), delimiter=',')
            _, header = next(reader), next(reader)
            info = self.info
            apply = [_convert(info[column]["type"]) if convert_dtypes else (lambda x: x) for column in header]
            header = [info[column]["name"] for column in header]
            return [header] + [[func(value) for func, value in zip(apply, row)] for row in reader]
        else:
            return list()

    @property
    def info(self) -> dict[str,dict]:
        return {k: v for kv in [self.ad_info, self.targeting, self.ad_performance, self.conv_performance, self.time] for k, v in kv.items()}

    @property
    def ad_info(self) -> dict[str,dict]:
        return { # 광고 정보
            "캠페인": dict(name="nccCampaignName", type="STRING"),
            "캠페인유형": dict(name="nccCampaignTp", type="STRING"),
            "광고그룹": dict(name="nccAdgroupName", type="STRING"),
            "광고그룹유형": dict(name="nccAdgroupTp", type="STRING"),
            "키워드": dict(name="keyword", type="STRING"),
            "소재": dict(name="nccAdId", type="STRING"),
            "소재 유형": dict(name="nccAdId", type="STRING"),
            "URL": dict(name="viewUrl", type="STRING"),
            "확장 소재": dict(name="nccAdExtensionId", type="STRING"),
            "확장 소재 유형": dict(name="nccAdExtTp", type="STRING"),
            "검색 유형": dict(name="schTp", type="STRING"),
            "검색어": dict(name="expKeyword", type="STRING"),
        }

    @property
    def targeting(self) -> dict[str,dict]:
        return { # 타겟팅 구분
            "매체이름": dict(name="mediaNm", type="STRING"),
            "PC/모바일 매체": dict(name="pcMblTp", type="STRING"),
            "검색/콘텐츠 매체": dict(name="ntwkTp", type="STRING"),
            "지역": dict(name="regnNo", type="STRING"),
            "상세지역": dict(name="regnR2Nm", type="STRING"),
            "성별": dict(name="criterionGenderNm", type="STRING"),
            "연령대": dict(name="criterionAgeTpNm", type="STRING"),
        }

    @property
    def ad_performance(self) -> dict[str,dict]:
        return { # 광고 성과
            "노출수": dict(name="impCnt", type="INTEGER"),
            "클릭수": dict(name="clkCnt", type="INTEGER"),
            "클릭률(%)": dict(name="ctr", type="FLOAT"),
            "평균클릭비용(VAT포함,원)": dict(name="cpc", type="INTEGER"),
            "총비용(VAT포함,원)": dict(name="salesAmt", type="INTEGER"),
            "평균노출순위": dict(name="avgRnk", type="FLOAT"),
            "동영상조회수": dict(name="viewCnt", type="INTEGER"),
            "반응수": dict(name="actCnt", type="INTEGER"),
        }

    @property
    def conv_performance(self) -> dict[str,dict]:
        return { # 전환 성과
            "전환수": dict(name="ccnt", type="INTEGER"),
            "직접전환수": dict(name="drtCcnt", type="INTEGER"),
            "간접전환수": dict(name="idrtCcnt", type="INTEGER"),
            "전환율(%)": dict(name="crto", type="FLOAT"),
            "전환매출액(원)": dict(name="convAmt", type="INTEGER"),
            "직접전환매출액(원)": dict(name="drtConvAmt", type="INTEGER"),
            "간접전환매출액(원)": dict(name="idrtConvAmt", type="INTEGER"),
            "전환당비용(원)": dict(name="cpConv", type="INTEGER"),
            "광고수익률(%)": dict(name="ror", type="FLOAT"),
            "전환유형": dict(name="convTp", type="STRING"),
            "방문당 평균페이지뷰": dict(name="pv", type="FLOAT"),
            "방문당 평균체류시간(초)": dict(name="stayTm", type="FLOAT"),
            "전환수(네이버페이)": dict(name="npCcnt", type="INTEGER"),
            "전환매출액(네이버페이)": dict(name="npConvAmt", type="INTEGER"),
        }

    @property
    def time(self) -> dict[str,dict]:
        return { # 시간구분
            "일별": dict(name="ymd", type="DATE"),
            "주별": dict(name="ww", type="STRING"),
            "요일별": dict(name="dayw", type="STRING"),
            "시간대별": dict(name="hh24", type="STRING"),
            "월별": dict(name="yyyymm", type="STRING"),
            "분기별": dict(name="yyyyqq", type="STRING"),
        }


class DailyReport(DuckDBTransformer):
    queries = ["create", "select", "insert"]

    def transform(self, obj: str, customer_id: int | str, **kwargs):
        report_csv = AdvancedReport().transform(obj, convert_dtypes=True)
        if len(report_csv) > 1:
            report_json = [dict(zip(report_csv[0], row)) for row in report_csv[1:]]
            self.insert_into_table(report_json, params=dict(customer_id=customer_id))
