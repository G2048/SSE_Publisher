import logging
from dataclasses import dataclass

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file='../.env', env_file_encoding='utf-8', extra='ignore')
    tn_events: str
    kafka_broker: str
    group_id: str
    debug: bool = False


@dataclass
class LoggerSettings:
    name: str = ''
    level: int = 20
    logging.basicConfig(
        format='%(asctime)s::%(levelname)s::%(filename)s::%(levelno)s::%(lineno)s::%(message)s', level=level
    )

    @property
    def logger(self):
        logger = logging.getLogger(name=self.name)
        if AppSettings().debug:
            self.level = 10
        logger.setLevel(self.level)
        return logger


class KafkaProducerCredentials(BaseModel):
    bootstrap_servers: str

    @property
    def conf(self):
        return {'bootstrap.servers': self.bootstrap_servers}


class KafkaConsumerCredentials(KafkaProducerCredentials):
    group_id: str

    @property
    def conf(self):
        return {'bootstrap.servers': self.bootstrap_servers, 'group.id': self.group_id}
