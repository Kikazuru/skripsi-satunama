# from psycopg2 import connection
import time
from datetime import datetime


def sql(connection, query: str, n=10):
    print(f"QUERY : {query}")

    result = []

    for i in range(1, n + 1):
        print(f"RUN {i}".center(20).replace(" ", "="))
        cursor = connection.cursor()

        start = time.time()

        cursor.execute(query)

        end = time.time()

        execution_time = round((end - start) * 1000, 4)

        result.append({
            "no_run": i,
            "run_start": datetime.fromtimestamp(start),
            "run_end": datetime.fromtimestamp(end),
            "execution_time": execution_time
        })

        cursor.close()
    return result
