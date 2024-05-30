"""spark stream
"""
import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_keyspace(session):
    """Create keyspace here

    Args:
        session (_type_): Cassandra session
    """
    session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS spark_streams
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'
                    }
                    """)
    print("Keyspace created successfully!")

def create_table(session):
    """ Create table here

    Args:
        session (_type_): Cassandra session
    """
    session.execute("""
                    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                    id UUID PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    gender TEXT,
                    address TEXT,
                    post_code TEXT,
                    email TEXT,
                    username TEXT,
                    registered_date TEXT,
                    phone TEXT,
                    picture TEXT)
                    """)
    print("Table created successfully!")

def insert_data(session, **kwargs):
    """ Insertion here

    Args:
        session (Cassandra session): _description_
    """    
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')

def create_spark_connection():
    """ Creating spark connection

    Returns:
        spark_conn: SparkSession
    """
    spark_conn = None
    try:
        spark_conn = SparkSession.builder \
                        .appName("Spark Data Steaming") \
                        .config("spark.jars.packages",
                                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
                        .config("spark.cassandra.connection.host", "cassandra") \
                        .getOrCreate()
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error("Couldn't create the spark session due to exception %s", e)

    return spark_conn

def connect_to_kafka(spark_conn):
    """ Creating kafka connection

    Args:
        spark_conn (SparkSession): Spark Connection

    Returns:
        spark_df: Spark stream data frame
    """    
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_cassandra_connection():
    """ Creating cassandra connection

    Returns:
        cas_session: Cassandra Session
    """
    cas_session = None

    try:
        cluster = Cluster(["cassandra"])

        cas_session = cluster.connect()
    except Exception as e:
        logging.error("Could not create cassandra connection due to %s", e)

    return cas_session

def create_selection_df_from_kafka(spark_df):
    """ Structuring  data frame

    Args:
        spark_df (Data Frame): Stream data frame

    Returns:
        sel: Stream data frame have structure
    """
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")
            
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option("checkpointLocation", "/tmp/checkpoint")
                               .option("keyspace", "spark_streams")
                               .option("table", "created_users")
                               .start())

            streaming_query.awaitTermination()