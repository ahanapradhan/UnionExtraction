from ....src.core.executable import Executable
from ....src.core.nullfree_executable import NullFreeExecutable


class ExecutableFactory:
    _instance = None
    app = None

    # For maintaining a singleton class
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ExecutableFactory, cls).__new__(cls)
        return cls._instance

    def create_exe(self, connectionHelper):
        detect_outer_join = connectionHelper.config.detect_oj
        if detect_outer_join:
            obj = NullFreeExecutable(connectionHelper)
        else:
            obj = Executable(connectionHelper)
        self.app = obj
        return obj
