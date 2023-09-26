import logging

from mysite.unmasque.src.util.constants import LOG_FORMAT


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
    logging.basicConfig(format=LOG_FORMAT)

    def __init__(self, name, level):
        super().__init__(name, level)

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
