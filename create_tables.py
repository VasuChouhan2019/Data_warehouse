import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
# from sql_queries, importing SQL queries contained in list and then passing those list in funtions defind below to run.


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
        cur.execute(query)
        # Execute all the queries one by one that stored in list drop_table_queries
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
        cur.execute(query)
        # Execute all the queries one by one that stored in list create_table_queries
        conn.commit()


def main():
    """
    Main function to set up the ETL process.

    This function reads configuration settings, establishes a connection to the PostgreSQL
    database, and calls functions to load staging tables and insert data into the final tables.

    Returns:
    None
    """
    
     # Read the configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Establish a connection to the database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drop table if existed
    drop_tables(cur, conn)
    # Create table
    create_tables(cur, conn)

    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()
