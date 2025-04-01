import time

import oracledb
import requests
from kafka import KafkaProducer
from loki_logging_lib import loki_handler

from settings import *

logger = loki_handler.setup_logger(LOKI_URL, LOKI_JOB_NAME)


def get_sql_scripts(connection):
    sqls = {}
    logger.info('Получение SQL-скриптов.')
    with connection.cursor() as cursor:
        for r_sql in cursor.execute("SELECT s.CODE, s.SCRIPT "
                                    "FROM APP_APEX_MICROSERVICES.V_PYTHON_SQL s "
                                    "WHERE s.PROJECT_NAME = 'argocd_events_stream' "
                                    "ORDER BY s.ID "):
            sqls[str(r_sql[0])] = ''.join(r_sql[1].read())
    return sqls


with oracledb.connect(user=APEX_USER,
                      password=APEX_PASSWORD,
                      dsn=APEX_DSN) as connection_apex:
    sql_list = get_sql_scripts(connection_apex)
    with connection_apex.cursor() as cursor_apex:
        cursor_apex.execute(sql_list['SELECT_ARGOCD_PARAMETERS'],
                            argoid=ARGOID)
        rows = cursor_apex.fetchall()
        logger.info('Получение ARGOCD параметров.')

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)

while True:
    try:
        r = requests.get(rows[0][0],
                         stream=True,
                         headers={"Authorization": "Bearer " + rows[0][1]},
                         timeout=ARGO_TIMEOUT)

        if r.encoding is None:
            r.encoding = 'utf-8'

        for line in r.iter_lines(decode_unicode=True):
            if line:
                producer.send(topic=TOPIC,
                              key=bytes(ARGOID, 'utf-8'),
                              value=bytes(line, 'utf-8'))
                producer.flush()
    except requests.exceptions.RequestException as e:
        logger.warning(f"Переподключение через пять секунд. "
                       f"Не было новых действий уже {ARGO_TIMEOUT} секунд.")
