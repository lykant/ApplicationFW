from pathlib import Path
import activity as act
import lib._types as typ
import lib._exc as exc
import argparse as arp
import warnings


warnings.filterwarnings("ignore")


class Application:
    path = None
    header = None
    argument = None
    loader = None
    menu = None
    _uc = None
    _activ = None
    _parser = None

    def __init__(self, path: str, header: str = "APPLICATION"):
        self.path = path
        self.header = header
        # Constant objects
        self._activ = act.Activity(self.path)
        self._parser = arp.ArgumentParser()
        self.setup()

    def setup(self) -> None:
        # Objects which can be changed are here.
        self._uc = typ.BaseUserChoice()

    def run(self) -> bool:
        try:
            if self.argument:
                self.argument.execute()
            if self.menu:
                self.menu.execute(self._activ)
            self.loader.execute(self._activ)

            return True
        except exc.Canceled as err:
            self._activ.canceled(err)
            return False
        except KeyboardInterrupt as err:
            self._activ.user_interrupt(exc.UserInterrupt())
            return False
        except Exception as err:
            self._activ.interrupted(err)
            return False
        finally:
            print()


# ***********************
if __name__ == "__main__":
    Application(path=Path(__file__)).run()
