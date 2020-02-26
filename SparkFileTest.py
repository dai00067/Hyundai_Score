from pyhive import hive
import pyhs2

with pyhs2.connect(host='10.104.111.33',
                   port=10000,
                   authMechanism="PLAIN",
                   user='bestfei',
                   password='password',
                   database='default') as conn:
    with conn.cursor() as cur:
        # Show databases
        # Execute query
        cur.execute("show databases")
        # Return column info from query
        # Fetch table results
        for i in cur.fetch():
            print(i)