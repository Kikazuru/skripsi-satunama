# from psycopg2 import connection
import time


def sql(connection, query: str, n=10):
    print(f"QUERY : {query}")

    result = []

    for i in range(1, n + 1):
        print(f"RUN {i}".center(20).replace(" ", "="))
        cursor = connection.cursor()

        start = time.perf_counter()

        cursor.execute(query)

        end = time.perf_counter()

        execution_time = round((end - start) * 1000, 4)

        result.append(execution_time)

        cursor.close()
    return result
