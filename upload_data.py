import os
import time
import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from io import StringIO

def create_db_connection():
    host= os.environ.get('DB_HOST','db')
    database= os.environ.get('DB_NAME','mydatabase')
    user= os.environ.get('DB_USER','user')
    password= os.environ.get('DB_PASSWORD','password')

    conn= None
    retries= 5
    for i in range(retries):
        try:
            conn= psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )
            print('Connection to PostgreSQL DB successful')
            return conn
        except OperationalError as e:
            print(f'{i+1} attempt filed due to {e}')
            time.sleep(2**i)
    print('Connection to PostgreSQL DB failed')

def execute_query(conn,query):
    cur = conn.cursor()
    try:
        cur.execute(query)
        conn.commit()
        print('Query executed successfully')
    except OperationalError as e:
        print(f'Error executing query: {e}')
        cur.rollback()
    finally:
        cur.close()

def read_query(conn,query):
    cur = conn.cursor()
    try:
        cur.execute(query)
        result = cur.fetchall()
        return result
    except OperationalError as e:
        print(f'Error executing query: {e}')
    finally:
        cur.close()

def csv_to_db(conn):
    df=pd.read_csv('output.csv')
    print('Data has been loaded :')
    print(df.head(5))
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cur = conn.cursor()
    try:
        cur.copy_from(buffer, 'stock_data', sep=",")
        conn.commit()
    except OperationalError as e:
        print(f'Error copying data to database: {e}')
        conn.rollback()
    finally:
        cur.close()

def main():
    print("App startteds")

    conn=create_db_connection()
    if not conn:
     return
    
    query="""
    CREATE TABLE IF NOT EXISTS stock_data (
        date DATE,
        name VARCHAR(255),
        u_close_price VARCHAR(255),
        u_prev_close_price VARCHAR(255),
        u_log_returns VARCHAR(255),
        u_prev_day_volatility VARCHAR(255),
        cdu_daily_volatility VARCHAR(255),
        u_annualised_volatility VARCHAR(255)
    );
    """
    execute_query(conn,query)
    csv_to_db(conn)
    conn.close()
    print('Connection to PostgreSQL DB closed')

if __name__ == '__main__':
    main()


