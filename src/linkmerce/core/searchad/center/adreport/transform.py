from __future__ import annotations

from linkmerce.common.transform import ExcelTransformer, DuckDBTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable, Literal, TypeVar
    import datetime as dt
    ColumnKr = TypeVar("ColumnKr", bound=str)
    ColumnEn = TypeVar("ColumnEn", bound=str)
    ColumnType = Literal["STRING", "FLOAT", "INTEGER", "DATE"]


def _cast(type: ColumnType) -> Callable[[str], str | float | int | dt.date]:
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


class AdvancedReport(ExcelTransformer):
    """네이버 검색광고 다차원 보고서 (CSV 형식) 데이터를 파싱하는 클래스.

    주요 설정 변수:
    - `header` - Excel 헤더 행 번호 (1부터 시작)
    - `columns` - 한국어 칼럼명 (기본값 `['*']` -> 원본 칼럼명 추출)
    - `convert_dtypes` - 데이터 타입 변환 여부 (기본값 `True`)"""

    columns: list[ColumnKr] = ['*']
    convert_dtypes: bool = True

    def pre_init(
            self,
            header: int | None = None,
            columns: list[tuple] | None = None,
            convert_dtypes: bool | None = None,
            **kwargs
        ):
        if header is not None:
            self.header = header
        self.set_columns(columns)
        if convert_dtypes is not None:
            self.convert_dtypes = convert_dtypes

    def set_columns(self, columns: list[tuple] | None = None):
        """칼럼 목록을 설정한다. 칼럼 목록에 `*`이 없으면 특정 칼럼만 선택하도록 `fields`를 추가로 설정한다."""
        if columns is not None:
            self.columns = columns
        if '*' not in self.columns:
            self.fields = list(self.get_columns(self.columns)[0])

    def parse(self, obj: str, **kwargs) -> list[dict]:
        """CSV 문자열을 읽어 헤더를 영어 칼럼명으로 매핑하고, `convert_dtypes` 여부에 따라 형변환한다."""
        from io import StringIO
        import csv
        data = list()

        if obj:
            reader = csv.reader(StringIO(obj), delimiter=',')
            columns, types = self.get_columns(*[next(reader) for _ in range(self.header)])
            functions = [_cast(dtype) if self.convert_dtypes else (lambda x: x) for dtype in types]
            for row in reader:
                values = [cast(value) for cast, value in zip(functions, row)]
                data.append(dict(zip(columns, values)))
        return data

    def get_columns(self, *headers: list[ColumnKr]) -> tuple[list[ColumnEn], list[ColumnType]]:
        """마지막 헤더 행을 기준으로 영어 칼럼명 및 데이터 타입 매핑을 반환한다."""
        total = list()
        for name in ["ad_info", "targeting", "ad_performance", "conv_performance", "time"]:
            attr: dict[ColumnKr, tuple[ColumnEn, ColumnType]] = getattr(self, name)
            total += list(attr.items())
        attrs = dict(total)
        return tuple(zip(*[attrs[name] for name in headers[-1]]))

    @property
    def ad_info(self) -> dict[ColumnKr, tuple[ColumnEn, ColumnType]]:
        """광고 정보 - 칼럼 명칭 및 타입"""
        return {
            "캠페인": ("nccCampaignName", "STRING"),
            "캠페인 유형": ("nccCampaignTp", "STRING"),
            "광고그룹": ("nccAdgroupName", "STRING"),
            "광고그룹 유형": ("nccAdgroupTp", "STRING"),
            "키워드": ("keyword", "STRING"),
            "소재": ("nccAdId", "STRING"),
            "소재 유형": ("nccAdId", "STRING"),
            "URL": ("viewUrl", "STRING"),
            "확장 소재": ("nccAdExtensionId", "STRING"),
            "확장 소재 유형": ("nccAdExtTp", "STRING"),
            "검색 유형": ("schTp", "STRING"),
            "검색어": ("expKeyword", "STRING"),
        }

    @property
    def targeting(self) -> dict[ColumnKr, tuple[ColumnEn, ColumnType]]:
        """타겟팅 구분 - 칼럼 명칭 및 타입"""
        return {
            "매체이름": ("mediaNm", "STRING"),
            "PC/모바일 매체": ("pcMblTp", "STRING"),
            "검색/콘텐츠 매체": ("ntwkTp", "STRING"),
            "지역": ("regnNo", "STRING"),
            "상세지역": ("regnR2Nm", "STRING"),
            "성별": ("criterionGenderNm", "STRING"),
            "연령대": ("criterionAgeTpNm", "STRING"),
        }

    @property
    def ad_performance(self) -> dict[ColumnKr, tuple[ColumnEn, ColumnType]]:
        """광고 성과 - 칼럼 명칭 및 타입"""
        return {
            "노출수": ("impCnt", "INTEGER"),
            "클릭수": ("clkCnt", "INTEGER"),
            "클릭률(%)": ("ctr", "FLOAT"),
            "평균 CPC": ("cpc", "INTEGER"),
            "총비용": ("salesAmt", "INTEGER"),
            "평균노출순위": ("avgRnk", "FLOAT"),
            "총 재생수": ("viewCnt", "INTEGER"),
            "반응수": ("actCnt", "INTEGER"),
        }

    @property
    def conv_performance(self) -> dict[ColumnKr, tuple[ColumnEn, ColumnType]]:
        """전환 성과 - 칼럼 명칭 및 타입"""
        return {
            "총 전환수": ("ccnt", "INTEGER"),
            "직접전환수": ("drtCcnt", "INTEGER"),
            "간접전환수": ("idrtCcnt", "INTEGER"),
            "총 전환율(%)": ("crto", "FLOAT"),
            "총 전환매출액(원)": ("convAmt", "INTEGER"),
            "직접전환매출액(원)": ("drtConvAmt", "INTEGER"),
            "간접전환매출액(원)": ("idrtConvAmt", "INTEGER"),
            "총 전환당비용(원)": ("cpConv", "INTEGER"),
            "총 전환당비용(원)": ("ror", "FLOAT"),
            "전환 유형": ("convTp", "STRING"),
            # "방문당 평균페이지뷰": ("pv", "FLOAT"),
            "pv": ("pv", "FLOAT"),
            # "방문당 평균체류시간(초)": ("stayTm", "FLOAT"),
            "stayTm": ("stayTm", "FLOAT"),
            # "총 전환수(네이버페이)": ("npCcnt", "INTEGER"),
            # "총 전환매출액(네이버페이)": ("npConvAmt", "INTEGER"),
        }

    @property
    def time(self) -> dict[ColumnKr, tuple[ColumnEn, ColumnType]]:
        """시간구분 - 칼럼 명칭 및 타입"""
        return {
            "일별": ("ymd", "DATE"),
            "주별": ("ww", "STRING"),
            "요일별": ("dayw", "STRING"),
            "시간대별": ("hh24", "STRING"),
            "월별": ("yyyymm", "STRING"),
            "분기별": ("yyyyqq", "STRING"),
        }


class DailyReport(DuckDBTransformer):
    """네이버 검색광고 다차원 보고서를 일별로 구분하여 `searchad_report` 테이블에 적재하는 클래스."""

    extractor = "DailyReport"
    tables = {"table": "searchad_report"}
    parser = AdvancedReport
    parser_config = dict(
        header = 2,
        columns = [
            "소재 유형", "매체이름", "PC/모바일 매체", "검색/콘텐츠 매체", "노출수", "클릭수",
            "총비용(VAT포함,원)", "총 전환수", "직접전환수", "총 전환매출액(원)", "직접전환매출액(원)",
            "평균노출순위", "pv", "stayTm", "일별"
        ],
    )
    params = {"customer_id": "$customer_id"}
