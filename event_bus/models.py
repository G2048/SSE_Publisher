from datetime import datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_serializer


class EventModel(BaseModel):
    timestamp: datetime = Field(default_factory=datetime.now)
    key: str = None
    message: str
    transaction_id: UUID = Field(default_factory=uuid4)

    @field_serializer('timestamp')
    def timestamp_to_str(self, value, _info):
        return datetime.strftime(value, '%Y-%m-%d %H:%M:%S')

    @field_serializer('transaction_id')
    def transaction_id_to_str(self, value, _info):
        return str(value)


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
