import logging
import sys
from logging.handlers import TimedRotatingFileHandler


class LoggerSmwj:

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    formatter = logging.Formatter('[%(levelname)s:%(lineno)s] %(asctime)s > %(message)s')
    logger = logging.getLogger()

    fh = TimedRotatingFileHandler("../logs", when="midnight")
    fh.setFormatter(formatter)
    fh.suffix = "_%Y%m%d.log"

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.setLevel(logging.DEBUG)