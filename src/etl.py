import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load the JSON data from S3 to staging tables in Redshift
       as specified in list of queries, copy_table_queries.

    Args
        cur: cursor object, cursor bound to the connection that allows SQL to execute
        conn: connection object, connection to a Redshift cluster instance

    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Load the data from staging tables in Redshift to final tables in Redshift
       as specified in list of queries, insert_table_queries.

    Args
        cur: cursor object, cursor bound to the connection that allows SQL to execute
        conn: connection object, connection to a Redshift cluster instance

    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Transform and load data from JSON files in S3 to a final table in Redshift."""

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    print('Data from S3 loaded into Redshift cluster successfully')

    conn.close()


if __name__ == "__main__":
    main()
