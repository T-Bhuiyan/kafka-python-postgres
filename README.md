# kafka-python-postgres

This demo application is able to send events to a Kafka topic (a producer) which will then be read
by a Kafka consumer. The consumer application then store the consumed data to an PostgreSQL database.

## Application Overview

This repository contains the following files.

* [00 - main.py](https://github.com/T-Bhuiyan/kafka-python-postgres/blob/main/main.py) : Run as the entry point of this application.
* [01 - ConnectionFactory.py](https://github.com/T-Bhuiyan/kafka-python-postgres/blob/main/ConnectionFactory.py) : Handles all the connection and permission regarding postgres.
* [02 - KafkaFactory.py](https://github.com/T-Bhuiyan/kafka-python-postgres/blob/main/KafkaFactory.py) : Creates a Python Producer and produces the messages then creates a Consumer to reads messages produced by Producer. 

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) for installation.

```bash
pip install kafka-python

pip install psycopg2 
```

## Usage

To run this python application:
```python
python3 <filename>
OR,
python3 -m <modulename>

```

## Keep Reading

There are some other resources that you may also find useful:

* https://aiven.io/blog/create-your-own-data-stream-for-kafka-with-python-and-faker
* https://help.aiven.io/en/articles/5343895-python-examples-for-testing-aiven-for-apache-kafka
* https://www.postgresqltutorial.com/postgresql-python/create-tables/
* https://www.postgresqltutorial.com/postgresql-python/insert/

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

This project is licensed under the [MIT](https://choosealicense.com/licenses/mit/)