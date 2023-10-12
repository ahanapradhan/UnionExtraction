import logging
from pathlib import Path


def get_format_args(msg, args):
    f_msg = format(str(msg))
    f_args = [f_msg]
    for arg in args:
        f_args.append(format(str(arg)))
    f_msg = ""
    for i in range(len(f_args)):
        f_msg += "%s "
    return f_msg, f_args


class Log(logging.Logger):
    # creating a formatter
    formatter = logging.Formatter('- %(name)s - %(levelname)-8s: %(message)s')

    def __init__(self, name, level):
        super().__init__(name, level)
        # ch = logging.StreamHandler()
        # ch.setLevel(level)
        # ch.setFormatter(self.formatter)
        # self.addHandler(ch)
        self.base_path = Path(__file__).parent.parent.parent.parent
        log_file = (self.base_path / "unmasque.log").resolve()
        fh = logging.FileHandler(log_file, 'a')
        fh.setLevel(level)
        fh.setFormatter(self.formatter)
        self.addHandler(fh)

    def debug(self, msg, *args):
        f_msg, f_args = get_format_args(msg, args)
        super().debug(f_msg, *f_args)

    def error(self, msg, *args):
        f_msg, f_args = get_format_args(msg, args)
        super().error(f_msg, *f_args)

    def info(self, msg, *args):
        f_msg, f_args = get_format_args(msg, args)
        super().info(f_msg, *f_args)

    def warning(self, msg, *args):
        f_msg, f_args = get_format_args(msg, args)
        super().info(f_msg, *f_args)
