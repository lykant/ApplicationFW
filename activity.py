from pathlib import Path
from logengine import log
import lib.adate as dfn
import lib.atext as txt
import logengine as len
import lib._types as typ
import lib._exc as exc
import config
import time
import os


class Activity:
    path = None
    run_id = None
    __start_time = None
    __finish_time = None
    __db_shown = False

    def __init__(self, path: Path = None):
        self.path = path
        if self.path is not None:
            log(self.path).initialize()

    def setup(self, channel: len.chlog = len.chlog.FILE, run_id: int = None):
        log().channel = channel
        if run_id:
            self.run_id = run_id

    @property
    def load_footer(self) -> str:
        msg = ""
        if self.run_id is not None:
            msg = f"Run id: {self.run_id} -> "
        time_txt = f"{self.__start_time} & {self.__finish_time}"
        msg = f"{msg}{time_txt}"
        return msg

    def clear_screen(self) -> None:
        os.system("cls")

    def print_formatted_header(self, header: str) -> None:
        log().status(txt.bf_star(f"<< {header.upper()} >>", center=True))
        if not self.__db_shown:
            log().status(txt.bf_eq(f"Database: {config.DB_NAME}", center=True))
            self.__db_shown = True

    def print_menu_header(self, header: str, info: str = None) -> None:
        self.print_formatted_header(header)
        if info is not None:
            log().status(txt.bf_dash(info))
        log().status(txt.equal() + "\n")

    def ask_quit(self) -> bool:
        import msvcrt

        log().status(txt.nr_dash(">> Press any key to start or ESC to quit."))
        if msvcrt.getch().decode() == chr(27):
            return True
        return False

    def print_load_header(self, header: str) -> None:
        self.__start_time = dfn.get_now_str()
        self.clear_screen()
        self.print_formatted_header(header)

    def display_choices(self, xdc: typ.BaseUserChoice, print_equal: bool = True) -> None:
        if xdc is None:
            return
        log().status(txt.bf_dash(xdc.get_message(), center=True))
        if print_equal:
            log().status(txt.equal())

    def print_load_footer(self) -> None:
        self.__finish_time = dfn.get_now_str()
        log().status(txt.bf_eq(self.load_footer))

    def db_inserting(self) -> None:
        log().status(">> Db inserting:", end=" ")

    def excel_writing(self) -> None:
        log().status(">> Excel writing:", end=" ")

    def table_deleting(self, table: str) -> None:
        log().status(f">> {table} deleting:", end=" ")

    def table_deleted(self, row_count: int) -> None:
        log().status(f"Deleted. (Row count: {row_count})" if row_count > 0 else "No data.")

    def print_section(self, title: str = None, no: int = None) -> None:
        msg = None
        if title is not None:
            msg = f"{no or '#'}> {title}\n-"
        log().status(txt.bf_dash(msg))

    def loading(self, data_name: str) -> None:
        log().status(f">> {data_name.capitalize()} loading:", end=" ")

    def wait(self, delay_time: float = 0.0) -> None:
        if delay_time > 0.0:
            time.sleep(delay_time)

    def completed(self, row_count: int = None) -> None:
        msg = f"Completed."
        if row_count:
            msg = f"{msg} (Row count: {row_count})"
        log().status(msg)

    def no_data(self) -> None:
        log().status("No data!")

    def not_enough_data(self) -> None:
        log().status("Not enough data!")

    def display_sample(self, db_target) -> None:
        def ask_display() -> str:
            print(txt.dash())
            answer = str.upper(input("Display sample? [Y/N]: "))
            return answer

        if db_target is None:
            return
        if ask_display() != 'Y':
            return
        txt_sample = db_target.take_sample()
        if txt_sample is None:
            raise exc.NoneResultError
        print(txt_sample)

    def excel_writing(self) -> None:
        log().status(">> Excel writing:", end=" ")

    def canceled(self, error: Exception) -> None:
        log().error(str(error))

    def user_interrupt(self, error: Exception) -> None:
        log().error(str(error))

    def interrupted(self, error: Exception) -> None:
        log().exception(error)

    def free_message(self, message: str = "", end: str = "\n") -> None:
        log().status(message, end=end)
