import logging
import os

from pyspark.sql import SparkSession

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'ERROR')
SPARK_LOG_LEVEL = os.environ.get('SPARK_LOG_LEVEL', 'ERROR')
log_level_code = getattr(logging, LOG_LEVEL.upper(), None)

if not isinstance(log_level_code, int):
    raise ValueError(f'Invalid log level: {LOG_LEVEL}')

logging.basicConfig(format='%(asctime)s %(levelname)s [%(name)s]: %(message)s',
                    datefmt='%Y-%m-%d %I:%M:%S %p', level=log_level_code)
logger = logging.getLogger(__name__)
