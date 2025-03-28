import json
from datetime import datetime, timedelta
from typing import Dict, Any

from kafka import KafkaConsumer
from loki_logging_lib import loki_handler
from oracle_connector import OracleDB

from settings import *

logger = loki_handler.setup_logger(loki_url=LOKI_URL, service_name=LOKI_JOB_NAME)

oracle_pool = OracleDB(user=APEX_USER, password=APEX_PASSWORD, dsn=APEX_DSN, logger=logger)


def get_sql_scripts() -> Dict[str, str]:
    """
    Загружает SQL-скрипты из базы данных.
    :return: Словарь с кодами и текстами SQL-скриптов.
    """
    sqls = {}
    for code, script in oracle_pool.execute_query_and_fetchall("""
        SELECT s.CODE, s.SCRIPT 
        FROM APP_APEX_MICROSERVICES.V_PYTHON_SQL s 
        WHERE s.PROJECT_NAME = 'argocd_events_stream' 
        ORDER BY s.ID
    """):
        sqls[str(code)] = script.read()
    logger.info('Получение SQL-скриптов из Oracle.')
    return sqls


def parse_timestamp(timestamp: str) -> datetime:
    """
    Преобразует строку времени в объект datetime.
    :param timestamp: Временная метка в формате ISO 8601.
    :return: Объект datetime.
    """
    cleaned_timestamp = timestamp.replace('T', ' ').replace('Z', '')
    try:
        return datetime.strptime(cleaned_timestamp, '%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        logger.error(f"Invalid timestamp format: {timestamp}. Error: {e}")
        raise


def process_message(message: Dict[str, Any], sql_list: Dict[str, str]):
    """
    Обрабатывает одно сообщение Kafka.
    :param message: Данные сообщения.
    :param sql_list: Список SQL-запросов.
    """
    try:
        aid = message['key'].decode('utf-8')

        logger.info(f'Начата обработка сообщения {aid} из Kafka')

        result = message['value'].get('result', {})
        jtype = result.get('type', '').upper()
        app = result.get('application', {})
        metadata = app.get('metadata', {})
        spec = app.get('spec', {})
        status = app.get('status', {})

        # Извлечение данных из JSON
        appid = metadata.get('name', '').lower()
        created = parse_timestamp(metadata.get('creationTimestamp', ''))
        destination = spec.get('destination', {})
        appnamespace = destination.get('namespace')
        appserver = destination.get('server')
        appproject = spec.get('project')
        source = spec.get('source', {})
        apppath = source.get('path')
        apprevision = source.get('targetRevision')
        appurl = source.get('repoURL')
        health = status.get('health', {})
        ahealth = health.get('status', '').lower()
        ats = parse_timestamp(metadata.get('creationTimestamp', ''))
        sync = status.get('sync', {})
        sync_status = sync.get('status')
        operation_state = status.get('operationState')

        # Логика обработки событий
        if ahealth == 'missing':
            jtype = 'DELETED'

        if jtype in ('ADDED', 'MODIFIED') and aid and appid:
            oracle_pool.execute_query_and_commit(
                sql_list['MERGE_ARGOCDAPPS'],
                aid=aid,
                appid=appid,
                appnamespace=appnamespace,
                appproject=appproject,
                appserver=appserver,
                apppath=apppath,
                apprevision=apprevision,
                appurl=appurl,
                ahealth=ahealth,
                ats=ats,
                sync_status=sync_status,
                created=created
            )

            if jtype == 'MODIFIED' and operation_state:
                started_at = operation_state.get('startedAt', '')
                try:
                    operations = parse_timestamp(started_at) + timedelta(hours=3)
                except Exception:
                    operations = datetime.now()

                if operations > datetime.now() - timedelta(minutes=1):
                    oracle_pool.execute_query_and_commit(sql_list['INSERT_ARGOCDSILENCEEVENTS'],
                                                         eventts=operations,
                                                         argoid=aid,
                                                         appid=appid)
            oracle_pool.execute_query_and_commit(
                sql_list['UPDATE_ARGOCD_CTX'],
                aid=aid
            )

        elif jtype == 'DELETED':
            oracle_pool.execute_query_and_commit(
                sql_list['DELETE_ARGOCDAPPS'],
                aid=aid,
                appid=appid
            )
            oracle_pool.execute_query_and_commit(
                sql_list['UPDATE_ARGOCD_CTX'],
                aid=aid
            )
        logger.info(f'Обработка сообщения {aid} из Kafka завершена.')

    except Exception as e:
        logger.error(f"Error processing Kafka message: {str(e)}")


def main():
    try:
        # Загрузка SQL-скриптов
        sql_list = get_sql_scripts()

        # Подключение к Kafka
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True
        )
        consumer.subscribe(TOPICS)

        logger.info("Started consuming messages from Kafka.")
        for raw_message in consumer:
            try:
                message = {
                    'key': raw_message.key,
                    'value': json.loads(raw_message.value.decode('utf-8'))
                }
                process_message(message, sql_list)
            except Exception as e:
                logger.error(f"Error processing Kafka message: {str(e)}")
    except Exception as e:
        logger.critical(f"Critical error occurred: {str(e)}")


if __name__ == "__main__":
    main()
