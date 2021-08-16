import ConnectionFactory
from kafka import KafkaProducer
import json
from config.kafka_config import *

class KafkaFactory():

    def __init__(self) -> None:
        self.producer = None
        self.consumer = None

    
    def Producer(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=hostname+":"+str(port),
            security_protocol="SSL",
            ssl_cafile=cert_folder+"/ca.pem",
            ssl_certfile=cert_folder+"/service.cert",
            ssl_keyfile=cert_folder+"/service.key",
            value_serializer=lambda v: json.dumps(v).encode('ascii'),
            key_serializer=lambda v: json.dumps(v).encode('ascii')
            )

    def Consumer(self) -> None:
        group_id = "avine_group"

        self.consumer = KafkaConsumer(
            client_id = "client1",
            group_id = group_id,
            bootstrap_servers = hostname+":"+str(port),
            security_protocol = "SSL",
            ssl_cafile = cert_folder+"/ca.pem",
            ssl_certfile = cert_folder+"/service.cert",
            ssl_keyfile = cert_folder+"/service.key",
            value_deserializer = lambda v: json.loads(v.decode('ascii')),
            key_deserializer = lambda v: json.loads(v.decode('ascii')),
            max_poll_records = 10
            )


    def produce_msg(self) -> None:
        self.Producer()
        self.producer.send(
        topic_name,
        key={"id":1},
        value={"name":"Francesco", "pizza":"Margherita"}
        )


        self.producer.send(
        topic_name,
        key={"id":2},
        value={"name":"Adele", "pizza":"Hawaii"}
        )
    
    def consume_msg(self) -> None:
        self.Consumer()
        self.consumer.topics()

        self.consumer.subscribe(topics=[topic_name])
        self.consumer.subscription()

        for message in consumer:
            print ("%d:%d: k=%s v=%s" % (message.partition,
                                        message.offset,
                                        message.key,
                                        message.value)
            ConnectionFactory().insert_data(message.key,message.value)
            
            



    

                                        
        