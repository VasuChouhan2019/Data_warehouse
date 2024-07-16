import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
"""
from sql_queries, importing SQL queries contained in list and then passing those list in funtions defind below to run.
"""

def drop_tables(cur, conn):
    """
    Drop table just to be sure so that no errors should pop if any table is already in Database.
    
    Parameters:
    cur (cursor): Database cursor object.
    conn (connection): Database connection object.

    Returns:
    None
    """
    for query in drop_table_queries:
            """
            As the queries are stored in drop_table_queries list so we need to use for loop so that all the quesies that stored in objects in list can run one by one. 
            """
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create table functions to create table in database.
    
    Parameters:
    cur (cursor): Database cursor object.
    conn (connection): Database connection object.

    Returns:
    None
    """
    for query in create_table_queries:
            """
            As the queries are stored in create_table_queries list so we need to use for loop so that all the quesies that stored in objects in list can run one by one. 
            """
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    """
    This object will be used to read from and interact with the configuration file.
    """
    config.read('dwh.cfg')
    """
    This method reads the configuration file which is dwh.cfg
    """

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    """
    This line creates a connection to a PostgreSQL database using the psycopg2.connect and using values from CLUSTER portion from dwh.cfg
    """
    cur = conn.cursor()
    """
    After establishing the connection, a cursor object is created using conn.cursor(). The cursor is used to interact with the database, allowing you to execute SQL queries and fetch results.
    """
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
