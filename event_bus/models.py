from pydantic import BaseModel


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
