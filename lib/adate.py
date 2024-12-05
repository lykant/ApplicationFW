from lib.db.engine import dbe
from lib.atable import atable
from datetime import datetime
from sqlalchemy import select
from sqlalchemy import column
from sqlalchemy import func
from typing import Union
import lib.common._exc as exc
import lib.common._constants as cons
import yfinance as yf
import pandas as pd
import numpy as np
import config


# *************************
# CONSTANTS
# *************************
# Formats
DATE_FORMAT_INT = "%Y%m%d"
DATE_FORMAT_STR = "%Y-%m-%d"

# Table model
P_MARKET_DATE = atable("P_MARKET_DATE")

# Columns
COL_MARKET_DATE = ["date", "date_no", "date_row", "pct_limit"]

# Constant
SAMPLE = "ZRGYO"

# *************************
# CLASSES
# *************************
# Market date class
class adate:
    date = date_no = date_row = None
    _db_table = P_MARKET_DATE
    _columns = COL_MARKET_DATE
    __df_market_date = pd.DataFrame()

    def __init__(self, date: datetime.date):
        self.get_market_date()
        self.date = self.get_correct_date(date)
        self.date_no = self.date_to_int(self.date)
        self.date_row = self.find_date_row(self.date)

    def __str__(self) -> str:
        return f"{self.date_no}"

    def __repr__(self) -> str:
        return f"adate({self.date!r})"

    def __eq__(self, other) -> bool:
        check_type(other)
        return self.date_no == other.date_no

    def __lt__(self, other) -> bool:
        check_type(other)
        return self.date_no < other.date_no

    def __le__(self, other) -> bool:
        check_type(other)
        return self.date_no <= other.date_no

    def __ne__(self, other) -> bool:
        check_type(other)
        return self.date_no != other.date_no

    def __ge__(self, other) -> bool:
        check_type(other)
        return self.date_no >= other.date_no

    def __gt__(self, other) -> bool:
        check_type(other)
        return self.date_no > other.date_no

    def __hash__(self) -> int:
        return hash(self.date_no)

    def get_next(self, n: int = 1):
        r_date = adate.get_next_st(self, n)
        return r_date

    def get_past(self, n: int = 1):
        r_date = adate.get_past_st(self, n)
        return r_date

    @classmethod
    def from_date_no(cls, date_no: int):
        return cls(date=cls.int_to_date(date_no))

    @classmethod
    def get_correct_date(cls, date: str = None, date_no: int = None) -> datetime.date:
        date_no = date_no or cls.date_to_int(date)
        r_date = date
        if not cls.get_market_date().empty:
            r_date = cls.str_to_date(
                cls.get_market_date().query("date_no <= @date_no")["date"].max()
            )
        return r_date

    @classmethod
    def get_correct(cls, date: str = None, date_no: int = None):
        return cls(cls.get_correct_date(date, date_no))

    @classmethod
    def create_sql(cls) -> str:
        list_column_meta = cls._db_table.get_meta_columns(cls._columns)
        sql_select = select(cls._db_table.meta)
        sql_select = sql_select.with_only_columns(*list_column_meta)
        sql_select = sql_select.order_by(column("date_no").desc())
        return sql_select

    @classmethod
    def get_market_date(cls) -> pd.DataFrame:
        if cls.__df_market_date.empty:
            sql_select = cls.create_sql()
            cls.__df_market_date = dbe().execute(sql_select)
        return cls.__df_market_date

    @classmethod
    def find_date_row(cls, date: str = None, date_no: int = None) -> int:
        date_no = cls.date_to_int(date) if date else date_no
        try:
            df_one_day = cls.get_market_date().query("date_no == @date_no")
            df_one_day.reset_index(drop=True, inplace=True)
            date_row = df_one_day["date_row"].iloc[0]
            return date_row
        except Exception:
            raise exc.MarketDateError(date_no)

    @classmethod
    def find_date_no(cls, date_row: int) -> int:
        try:
            df_one_day = cls.get_market_date().query("date_row == @date_row")
            df_one_day.reset_index(drop=True, inplace=True)
            date_no = df_one_day["date_no"].iloc[0]
            return date_no
        except Exception:
            raise exc.MarketDateError(date_row)

    @classmethod
    def find_date(cls, date_row: int) -> datetime.date:
        return cls.int_to_date(cls.find_date_no(date_row))

    @staticmethod
    def check_date_no(date_no: str) -> bool:
        try:
            _ = pd.to_datetime(date_no, format=DATE_FORMAT_INT).date()
            return True
        except Exception:
            return False

    @staticmethod
    def date_to_int(date: datetime.date) -> Union[int, pd.Series]:
        if date is None:
            raise exc.NoneParameterError()
        if isinstance(date, pd.Series):
            r_date_no = pd.to_datetime(date).dt.strftime(DATE_FORMAT_INT).astype(np.int64)
        else:
            r_date_no = int(pd.to_datetime(date).date().strftime(DATE_FORMAT_INT))
        return r_date_no

    @staticmethod
    def int_to_date(date_no: int) -> Union[datetime.date, pd.Series]:
        if date_no is None:
            raise exc.NoneParameterError()
        if isinstance(date_no, pd.Series):
            r_date = pd.to_datetime(date_no.astype(str), format=DATE_FORMAT_INT)
        else:
            r_date = pd.to_datetime(str(date_no), format=DATE_FORMAT_INT).date()
        return r_date

    @staticmethod
    def str_to_date(date: str) -> Union[datetime.date, pd.Series]:
        if date is None:
            raise exc.NoneParameterError()
        if isinstance(date, pd.Series):
            r_date = pd.to_datetime(date.astype(str), format=DATE_FORMAT_STR)
        else:
            r_date = pd.to_datetime(str(date), format=DATE_FORMAT_STR).date()
        return r_date

    @staticmethod
    def get_next_st(current, n: int = 1):
        check_type(current)
        r_date = adate.find_date(date_row=current.date_row + n)
        return adate(r_date)

    @staticmethod
    def get_past_st(current, n: int = 1):
        check_type(current)
        r_date = adate.find_date(date_row=current.date_row - n)
        return adate(r_date)

    @staticmethod
    def range(start, end):
        for p in locals().values():
            check_type(p)
        next = start
        while next <= end:
            yield next
            next = next.get_next()

    @staticmethod
    def generate_counter(start, count: int, next: bool = True):
        check_type(start)
        seq, i = (start, 1)
        while i <= count:
            yield seq
            seq = seq.get_next() if next else seq.get_past()


# *************************
# FUNCTIONS
# *************************
def check_type(a_date: adate) -> bool:
    if (a_date is not None) and (not isinstance(a_date, adate)):
        raise exc.IncorrectTypeError(a_date, "adate")
    return True


def get_now_str() -> str:
    return datetime.now().isoformat(timespec="seconds", sep=" ")


def calendar_first_date() -> datetime.date:
    return cons.CAL_FIRST_DATE


def calendar_first() -> adate:
    return adate(calendar_first_date())


def load_last_date() -> datetime.date:
    T_DAILY_PRICE = atable("T_DAILY_PRICE")
    f_max = func.max(T_DAILY_PRICE.get_column_meta("date_no")).label("max_date_no")

    sql_select = select(T_DAILY_PRICE.meta)
    sql_select = sql_select.with_only_columns(f_max)
    sql_select = sql_select.where(column("symbol").like(SAMPLE))

    max_date_no = dbe().execute(sql_select)["max_date_no"][0]
    load_last_date = adate.int_to_date(max_date_no) if max_date_no else cons.CAL_FIRST_DATE
    return load_last_date


def load_last() -> adate:
    return adate(load_last_date())


def market_last_date() -> datetime.date:
    df_hist = yf.Ticker(f"{SAMPLE}.{config.BIST_CODE}").history(period="1d")
    # TODO
    last_date = pd.to_datetime(df_hist.index).max().strftime(DATE_FORMAT_STR)
    if last_date is None:
        raise exc.NoneValueError("last_date")
    return last_date


def market_last() -> adate:
    return adate(market_last_date())


def backward_period(period: int, end: adate = None) -> tuple:
    check_type(end)
    if end is None:
        end = load_last()
    start = end.get_past(period - 1)
    if start > end:
        raise exc.InappropriateDateError(start.date_no, end.date_no)
    return (start, end)


def forward_period(period: int, start: adate = None) -> tuple:
    check_type(start)
    start = start or adate(cons.TODAY_DATE)
    end = start.get_next(period - 1)
    if start > end:
        raise exc.InappropriateDateError(start.date_no, end.date_no)
    return (start, start)


def find_day_count(start: adate, end: adate) -> int:
    for p in locals().values():
        check_type(p)
    return end.date_row - start.date_row
