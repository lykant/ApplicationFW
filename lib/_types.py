from abc import ABCMeta
from dataclasses import dataclass, asdict
import lib.atext as txt
import lib.common._constants as cons
import math

ITEM_SEP = "|"


@dataclass
class BaseUserChoice(metaclass=ABCMeta):
    list_printable: list = None
    has_arg: bool = False

    def get_message(self) -> str:
        item_sep = str.center(ITEM_SEP, 3, " ")
        list_item = self.__get_list_item()
        loop_count = math.ceil(len(list_item) / 3.0)
        txt_in_list = list()
        for i in range(0, loop_count):
            txt_in_list.append(
                str.center(item_sep.join(list_item[3 * i : 3 * (i + 1)]), cons.WIDE)
            )
        msg = f"\n{txt.dash()}\n".join(txt_in_list)
        return msg

    def __asdict(self) -> dict:
        dict_result = asdict(
            self, dict_factory=lambda x: {k: v for (k, v) in x if v is not None}
        )
        return dict_result

    def __get_list_item(self) -> list:
        list_item = list()
        for (k, v) in self.__asdict().items():
            if k not in self.list_printable:
                continue
            if k == "initial_run":
                list_item.append("-{}-".format("CDC Run" if v == "N" else "Initial Run"))
            else:
                list_item.append(f"{k.replace('_', ' ').title()}: {v}")
        return list_item
