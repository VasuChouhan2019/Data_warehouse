import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
# from sql_queries, importing SQL queries contained in list and then passing those list in funtions defind below to run.


def load_staging_tables(cur, conn):
    """
    This function will copy data from S3 to staging table in Redshift.
    
    Parameters:
    cur (cursor): Database cursor object.
    conn (connection): Database connection object.

    Returns:
    None
    """
    for query in copy_table_queries:
        cur.execute(query)
            # Execute all the queries one by one that stored in list copy_table_queries
        conn.commit()


def insert_tables(cur, conn):
    """
    This function will insert data in Facts and Dimension table from staging tables of Redshift.
    
    Parameters:
    cur (cursor): Database cursor object.
    conn (connection): Database connection object.

    Returns:
    None
    """
    for query in insert_table_queries:
        cur.execute(query)
            # Execute all the queries one by one that stored in list insert_table_queries
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

    # loading data from S3 to staging table
    load_staging_tables(cur, conn)
    # Insertng data from staging table to Fact and Dimension
    insert_tables(cur, conn)
   
    # Close the connection
    conn.close()



if __name__ == "__main__":
    main()
