import logging

SLOW_QUERY_LOG_MIN_DURATION = 15

query_logger = logging.getLogger('fireant.query_log')

slow_query_logger = logging.getLogger('fireant.slow_query_log')
