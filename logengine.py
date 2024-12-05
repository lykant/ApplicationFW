from pathlib import Path
from datetime import datetime
from enum import IntEnum
import lib.atext as txt
import lib.fileopr as fo
import lib.common._exc as exc
import pandas as pd
import logging, os
import config


# Constants
EXT_LOG = "log"
DIR_LOG = "_logging"
P_STT = "STT"
P_EXC = "EXC"
DATE_FORMAT = "%Y%m%d"

# Type
class chlog(IntEnum):
    SCREEN = 1
    FILE = 2


class LogEngine:
    __instance = None
    __status_file = None
    __exc_logger = None

    def __init__(self):
        raise exc.InstantiationError()

    @classmethod
    def instance(cls, app_path: Path):
        if app_path is not None:
            cls.__app_name = app_path.stem
            cls.__app_dir = os.path.dirname(app_path)
            cls.__log_dir = os.path.join(cls.__app_dir, DIR_LOG)

        if cls.__instance is None:
            cls.__instance = cls.__new__(cls)
        return cls.__instance

    @property
    def status_file(self) -> str:
        self.__status_file = self.__status_file or self.stt_file_path
        return self.__status_file

    @property
    def exc_logger(self) -> logging.Logger:
        self.__exc_logger = self.__exc_logger or self.__create_logger()
        return self.__exc_logger

    def initialize(self) -> None:
        self.channel = chlog.FILE
        self.exc_file_path = self.__make_file_path(P_EXC)
        self.stt_file_path = self.__make_file_path(P_STT)

    def status(self, text: str = "", end: str = "\n") -> None:
        self.__print(text, end)

    def error(self, text: str) -> None:
        self.__print(f"{txt.nr_tilde(text)}")

    def exception(self, error: Exception) -> None:
        self.error(str(error))
        self.exc_logger.error(error, exc_info=True)

    def __print(self, text: str, end: str = "\n") -> None:
        print(text, end=end, flush=True)
        if self.channel == chlog.FILE:
            with open(self.status_file, "a") as log:
                log.write(f"{text}{end}")

    def __create_logger(self, name: str = P_EXC) -> logging.Logger:
        logging.basicConfig(
            filename=self.exc_file_path,
            level=config.LOG_LEVEL,
            format="%(message)s",
        )
        return logging.getLogger(name)

    def __make_file_path(self, name: str) -> str:
        tdate_no = int(pd.to_datetime(datetime.today()).date().strftime(DATE_FORMAT))
        list_word = [name, self.__app_name, tdate_no]
        file_name = fo.make_file_name(list_word, file_ext=EXT_LOG)
        file_path = fo.make_full_path(self.__log_dir, file_name)
        return file_path


def log(app_path: Path = None) -> LogEngine:
    return LogEngine.instance(app_path)
