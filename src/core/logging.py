import json
import logging
import sys
import warnings

from core.config import settings

# setup logging
file_handler = logging.FileHandler(filename="./src/error.log", mode="a")
stream_handler = logging.StreamHandler(sys.stdout)
# # log formatting
formatter = logging.Formatter(
    json.dumps(
        {
            "ts": "%(asctime)s",
            "name": "%(name)s",
            "function": "%(funcName)s",
            "level": "%(levelname)s",
            "msg": json.dumps("%(message)s"),
        }
    )
)


file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

handlers = [file_handler if settings.ENV != "PRD" else None, stream_handler]
handlers = [h for h in handlers if h is not None]

logging.basicConfig(level=logging.DEBUG, handlers=handlers)

# set imported loggers to warning
logging.getLogger("aiomysql").setLevel(logging.ERROR)
logging.getLogger("aiokafka").setLevel(logging.WARNING)

# # https://github.com/aio-libs/aiomysql/issues/103
# # https://github.com/coleifer/peewee/issues/2229
# warnings.filterwarnings("ignore", ".*Duplicate entry.*")
