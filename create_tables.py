import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    # connect to default database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    except psycopg2.connect as e:
        print("Error: Could not make connection to the DEFAULT database")
        print(e)
        
    conn.set_session(autocommit=True)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Could not get cursor to the DEFAULT database")
        print(e)

    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:
            print("Could not drop database sparkifydb")
            print(e)
            
    try:
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e:
        print("Error: Could not create sparkifydb database")
        print(e)

    # close connection to default database
    try:
        conn.close()    
    except psycopg2.Error as e:
        print("Error in closing connection to default database")
        print(e)
    
    # connect to sparkify database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the sparkify database")
        print(e)
        
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the sparkify database")
        print(e)
    
    return cur, conn
# End of create_database

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()