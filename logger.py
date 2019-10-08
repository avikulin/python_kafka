import logging
import logging.handlers
import os
import sys
from enum import Enum
from pathlib import Path

class LogLevels(Enum):
    CRITICAL = 50
    FATAL = CRITICAL
    ERROR = 40
    WARNING = 30
    WARN = WARNING
    INFO = 20
    DEBUG = 10
    NOTSET = 0


class CustomLogger:
    def __init__(self, name: str, log_dir: str = None, log_level: LogLevels = None, log_format: str = None):
        if log_dir is None:
            log_dir = Path(f"{os.path.dirname(__file__)}/log")

        if log_level is None:
            log_level = LogLevels.INFO

        if log_format is None:
            log_format = "%(asctime)s - %(levelname)s - %(module)s - %(message)s"

        self.__name = name
        self.__log_dir = log_dir
        self.__level = log_level.value
        self.__fmt = log_format
        self.__logger = None

    def activate(self):
        self.__logger = self.__get_logger(self.__name, self.__log_dir, self.__level, self.__fmt)
        sys.excepthook = lambda exc_type, exc_value, exc_traceback: self.handle_exception(exc_type, exc_value,
                                                                                          exc_traceback)
        self.__logger.info("Application started.")

    @property
    def get(self) -> logging.Logger:
        return self.__logger

    def __get_logger(self, name, logdir, level, logformat):
        loggerName = name
        logFileName = os.path.join(logdir, f"{loggerName}.log")
        logger = logging.getLogger(loggerName)
        logger.setLevel(level)

        if not os.path.isdir(logdir):
            try:
                os.mkdir(logdir)
            except IOError as exc:
                raise IOError(f"ERROR ** Can't create the directory '{logdir}'**\n")
        i = 0
        while os.path.exists(logFileName) and not os.access(logFileName, os.R_OK | os.W_OK):
            i += 1
            logFileName = f"{logFileName.replace('.log', '')}.{str(i).zfill((len(str(i)) + 1))}.log"

        try:
            # fh = logging.FileHandler(logFileName)
            fh = logging.handlers.RotatingFileHandler(filename=logFileName, mode="a", maxBytes=1310720, backupCount=50)
        except IOError as exc:
            errOut = f"Unable to create/open log file {logFileName}.\n"
            if exc.errno is 13:  # Permission denied exception
                errOut += f"ERROR ** Permission Denied ** - {errOut}"
                raise OSError(errOut)

            elif exc.errno is 2:  # No such directory
                errOut += f"ERROR ** No such directory '{logdir}'** - {errOut}"

            elif exc.errno is 24:  # Too many open files
                errOut += f"ERROR ** Too many open files ** - Check open file descriptors in /proc/<PID>/fd/ (PID: {os.getpid()})"
                raise OSError(errOut)
            else:
                errOut += f"Unhandled Exception ** {str(exc)} ** - {errOut}"
                raise Exception(errOut)
        else:
            formatter = logging.Formatter(logformat)
            fh.setLevel(level)
            fh.setFormatter(formatter)
            logger.addHandler(fh)
        return logger

    def handle_exception(self, exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        self.__logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
