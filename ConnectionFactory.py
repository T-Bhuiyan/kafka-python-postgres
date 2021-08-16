import json
from config.kafka_config import *
import psycopg2

class ConnectionFactory():

    def __init__(self,pg_config_file : str) -> None:
        with open(pg_config_file) as json_file:
            data = json.loads(json_file.read().replace("'",'"'))
            self.pg_dbname = data['dbname']
            self.pg_host = data['host']
            self.pg_port = data['port']
            self.pg_super_user = data['user']
            self.pg_super_pwd = data['password']
        self.conn = None

    def connect(self) -> None:
        """
        establish connection to postgres
        """
        conn = psycopg2.connect(dbname=pg_dbname,
                        user=self.pg_super_user,
                        host=self.pg_host,
                        port=self.pg_port,
                        password=self.pg_super_pwd,
                        sslmode='require')
        self.conn = conn


        """
        granting permission to  db and schema
        """
    def grant_permission(self) -> None:
        self.connect()
        cur = self.conn.cursor()
        cur.execute("GRANT CONNECT ON DATABASE defaultdb TO "+self.pg_user+";")
        cur.execute("GRANT USAGE ON SCHEMA public TO "+self.pg_user+";")
        cur.close()

    def create_tables(self) -> None:
        commands = (
            """
            CREATE TABLE customers (
                id SERIAL PRIMARY KEY,
                uid INT NOT NULL,
                info VARCHAR(255) NOT NULL
            ) """
        self.execute(commands)

    def execute(self,commands : str) -> None:
        try:
            # connect to the PostgreSQL server
            cur = self.conn.cursor()
            # create table one by one
            for command in commands:
                cur.execute(command)
            # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if self.conn is not None:
                self.conn.close()


    def insert_data(self, uid, info) -> None:

        sql = """INSERT INTO customers (uid,info)
             VALUES(%s,%s) RETURNING id;"""
        self.execute(sql,uid,info)

    def execute_insert(self, sql, uid, info : str) -> None:
        try:
            # create a new cursor
            cur = self.conn.cursor()
            # execute the INSERT statement
            cur.execute(sql, (uid, info))
            # get the generated id back
            customer_id = cur.fetchone()[0]
            # commit the changes to the database
            conn.commit()
            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    

    


