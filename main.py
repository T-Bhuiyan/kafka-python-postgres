from ConnectionFactory import *
from KafkaFactory import *



if __name__ = '__main__':
    config_file_path = 'config/pg_config.json'
    
    conn = ConnectionFactory(config_file_path)
    conn.grant_permission()
    conn.kafka_connect_setup()
    conn.create_tables()

    kafka_conn = KafkaFactory()
    kafka_conn.produce_msg()
    kafka_conn.consume_msg()




