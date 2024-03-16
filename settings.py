import logging
from dataclasses import dataclass

from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file='../.env', env_file_encoding='utf-8', extra='ignore')
    topic: str
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
