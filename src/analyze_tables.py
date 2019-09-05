import configparser
import psycopg2
from sql_queries import analyze_table_queries

def analyze_tables(cur, conn):
    """Analyze the data in Redshift cluster
       as specified in list of queries, analyze_table_queries.

    Args
        cur: cursor object, cursor bound to the connection that allows SQL to execute
        conn: connection object, connection to a Redshift cluster instance

    """
    for query in analyze_table_queries:
        cur.execute(query)
        conn.commit()

        # print headers
        column_headers = []
        for column in cur.description:
            field_name = column[0]
            column_headers.append(field_name)
        print(column_headers)

        # print each row in result
        row = cur.fetchone()
        while row:
           print(row)
           row = cur.fetchone()

        print('-' * 5)


def main():
    """Connect to redshift cluster and run analytical queries."""

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    analyze_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
