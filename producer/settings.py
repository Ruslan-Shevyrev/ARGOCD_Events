import os
from configparser import ConfigParser

URL_CONF = 'config/config.ini'


def get_os_variable(name, config_param=None, config_name='db', default_value=None):
    """
    Получает значение переменной окружения или из конфигурационного файла.
    :param name: Имя переменной.
    :param config_param: Имя параметра в конфиге. По умолчанию = name
    :param config_name: Имя раздела в конфиге.
    :param default_value: Значение по умолчанию
    :return: Значение переменной.
    """
    if config_param is None:
        config_param = name
    try:
        return os.environ[name]
    except KeyError:
        config = ConfigParser()
        config.read(URL_CONF)
        try:
            return config[config_name][config_param]
        except KeyError as exc:
            if not default_value:
                raise exc
            return default_value


VAULT_URL = get_os_variable('VAULT_URL')
VAULT_CERT_PATH = get_os_variable('VAULT_CERT_PATH')

LOKI_URL = get_os_variable('LOKI_URL')
LOKI_JOB_NAME = get_os_variable('LOKI_JOB_NAME')

vault = val.Vault("argocd_events",
                  VAULT_CERT_PATH,
                  VAULT_URL,
                  LOKI_URL)

APEX_USER = vault.get_secret('APEX_USER',
                             'apex-db/microservices')
APEX_PASSWORD = vault.get_secret('APEX_PASSWORD',
                                 'apex-db/microservices')

APEX_DSN = get_os_variable('APEX_DSN', 'DSN')
ARGOID = get_os_variable('ARGOID', default_value=1)
ARGO_TIMEOUT = int(get_os_variable('ARGO_TIMEOUT', default_value=10))
KAFKA_BOOTSTRAP_SERVER = get_os_variable('KAFKA_BOOTSTRAP_SERVER',
                                         default_value='oraapex-prod:9092')

TOPIC = 'ARGOCD_EVENTS'
