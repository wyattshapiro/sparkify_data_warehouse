import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop the tables in the Redshift cluster as specified by list of queries,
       drop_table_queries.

    Args
        cur: cursor object, cursor bound to the connection that allows SQL to execute
        conn: connection object, connection to a Redshift cluster instance

    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create the tables in the Redshift cluster as specified in list of queries,
       create_table_queries.

    Args
        cur: cursor object, cursor bound to the connection that allows SQL to execute
        conn: connection object, connection to a Redshift cluster instance

    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Reset the Redshift cluster tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

    print('Tables reset successfully')


if __name__ == "__main__":
    main()
