import lib.common._constants as cons
import pandas as pd
import re

# Characters
PLUS = "+"
STAR = "*"
DASH = "-"
EQ = "="
TILDE = "~"


def justify(message: str, center: bool = True, char: str = None, width: int = cons.WIDE) -> str:
    message = message or ""
    if char is None:
        txt = str.center(message, width) if center else message
    else:
        txt = str.center(message, width, char) if center else str.ljust(message, width, char)
    return txt


def dash(message: str = "", center: bool = True, width: int = cons.WIDE) -> str:
    return justify(message, center, DASH, width)


def star(message: str = "", center: bool = True, width: int = cons.WIDE) -> str:
    return justify(message, center, STAR, width)


def equal(message: str = "", center: bool = True, width: int = cons.WIDE) -> str:
    return justify(message, center, EQ, width)


def plus(message: str = "", center: bool = True, width: int = cons.WIDE) -> str:
    return justify(message, center, PLUS, width)


def tilde(message: str = "", center: bool = True, width: int = cons.WIDE) -> str:
    return justify(message, center, TILDE, width)


def crtlf(message: str, center: bool, after: bool) -> str:
    txt = ""
    if message is not None:
        txt = justify(message, center)
        txt = f"{txt}\n" if after else f"\n{txt}"
    return txt


def af_eq(message: str, center: bool = False) -> str:
    txt = f"{crtlf(message, center, after=True)}{equal()}"
    return txt


def bf_eq(message: str, center: bool = False) -> str:
    txt = f"{equal()}{crtlf(message, center, after=False)}"
    return txt


def af_star(message: str, center: bool = False) -> str:
    txt = f"{crtlf(message, center, after=True)}{star()}"
    return txt


def bf_star(message: str, center: bool = False) -> str:
    txt = f"{star()}{crtlf(message, center, after=False)}"
    return txt


def db_eq(message: str, center: bool = False) -> str:
    txt = af_eq(bf_eq(message, center))
    return txt


def af_dash(message: str, center: bool = False) -> str:
    txt = f"{crtlf(message, center, after=True)}{dash()}"
    return txt


def bf_dash(message: str, center: bool = False) -> str:
    txt = f"{dash()}{crtlf(message, center, after=False)}"
    return txt


def db_dash(message: str, center: bool = False) -> str:
    txt = af_dash(bf_dash(message, center))
    return txt


def af_plus(message: str, center: bool = False) -> str:
    txt = f"{crtlf(message, center, after=True)}{plus()}"
    return txt


def bf_plus(message: str, center: bool = False) -> str:
    txt = f"{plus()}{crtlf(message, center, after=False)}"
    return txt


def db_plus(message: str, center: bool = False) -> str:
    txt = af_plus(bf_plus(message, center))
    return txt


def nr_dash(message: str) -> str:
    txt = f"\n{dash(width=cons.NARROW)}\n{message}"
    return txt


def nr_tilde(message: str) -> str:
    txt = f"\n{tilde(width=cons.NARROW)}\n{message}"
    return txt


def cap_line(sr_text: str) -> str:
    if sr_text is None:
        return
    sr_text = sr_text.astype(str).str[0].str.upper() + sr_text.astype(str).str.slice(start=1)
    return sr_text
