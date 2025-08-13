

from datetime import datetime
import logging
import os
import logging.handlers
import os
import sys
import traceback
from datetime import datetime
from colorama import init, Fore, Style

LOG_DIR = os.path.dirname(__file__)
LOG_FILE = os.path.join(LOG_DIR, 'Agent_log.log')
MAX_LOG_SIZE = 10 * 1024  # 10KB for testing

class SizeRotatingFileHandler(logging.FileHandler):
    def __init__(self, filename, maxBytes, encoding=None, delay=False):
        super().__init__(filename, encoding=encoding, delay=delay)
        self.maxBytes = maxBytes
        self.formatter = None

    def emit(self, record):
        try:
            # Check before emit
            if os.path.exists(self.baseFilename) and os.path.getsize(self.baseFilename) >= self.maxBytes:
                print(f"[DEBUG] Rotating log file: {self.baseFilename} size={os.path.getsize(self.baseFilename)}")
                self.rotate_log()
        except Exception as e:
            print(f"Log rotation error: {e}")
        super().emit(record)
        try:
            # Check again after emit in case log entry pushed it over
            if os.path.exists(self.baseFilename) and os.path.getsize(self.baseFilename) >= self.maxBytes:
                print(f"[DEBUG] Rotating log file after emit: {self.baseFilename} size={os.path.getsize(self.baseFilename)}")
                self.rotate_log()
        except Exception as e:
            print(f"Log rotation error after emit: {e}")

    def rotate_log(self):
        self.close()
        now = datetime.now().strftime('%Y%m%d_%H%M%S')
        new_name = os.path.join(
            os.path.dirname(self.baseFilename),
            f"Agent_log_{now}.log"
        )
        try:
            if os.path.exists(self.baseFilename):
                os.rename(self.baseFilename, new_name)
        except Exception as e:
            print(f"Log rotation error during move: {e}")
        # Re-attach a new handler to the logger
        logger = logging.getLogger("AgentLogger")
        new_handler = SizeRotatingFileHandler(self.baseFilename, maxBytes=self.maxBytes, encoding='utf-8')
        new_handler.setFormatter(self.formatter)
        logger.handlers = [h for h in logger.handlers if not isinstance(h, SizeRotatingFileHandler)]
        logger.addHandler(new_handler)

def setup_logging():
    logger = logging.getLogger("AgentLogger")
    logger.setLevel(logging.DEBUG)
    # Remove all handlers
    for h in logger.handlers[:]:
        logger.removeHandler(h)
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    handler = SizeRotatingFileHandler(LOG_FILE, maxBytes=MAX_LOG_SIZE, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s')
    handler.setFormatter(formatter)
    handler.formatter = formatter
    logger.addHandler(handler)
    logger.propagate = False
    print(f"[DEBUG] setup_logging: Handler attached to {LOG_FILE}, maxBytes={MAX_LOG_SIZE}")
    print(f"[DEBUG] setup_logging: Logger handlers: {[type(h).__name__ for h in logger.handlers]}")

class Color:
    GREEN = '\033[92m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    END = '\033[0m'

def log_success(context, message):
    logger = logging.getLogger("AgentLogger")
    msg = f"✅ SUCCESS: {context} - {message}"
    logger.info(msg)
    print(f"{Color.GREEN}{msg}{Color.END}")

def log_error(context, message):
    logger = logging.getLogger("AgentLogger")
    msg = f"❌ ERROR: {context} - {message}"
    logger.error(msg, exc_info=True)
    print(f"{Color.RED}{msg}{Color.END}")

def log_info(context, message):
    logger = logging.getLogger("AgentLogger")
    msg = f"ℹ️ INFO: {context} - {message}"
    logger.info(msg)
    print(f"{Color.BLUE}{msg}{Color.END}")
