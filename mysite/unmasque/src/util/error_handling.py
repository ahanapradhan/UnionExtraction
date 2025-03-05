import traceback as tb

from .Log import Log


class UnmasqueError(Exception):
    def __init__(self, err, name, additional_info=None):
        super().__init__()
        self.err_code = err['code']
        self.message = err['msg']
        self.info = additional_info
        self.logger = Log(name, 'ERROR')

    def report_to_logger(self, logger):
        self.error_output = True
        logger.error(f"Error no {self.err_code}, Reason: {self.message}, other_info = {self.info}")
        if len(self.info) == 0:
            print(f"Error no {self.err_code}, Reason: {self.message}")
        else:
            print(f"Error no {self.err_code}, Reason: {self.message}, other_info = {self.info}")
        trace_logs = tb.format_tb(self.__traceback__)
        for log in trace_logs:
            logger.debug(log)
