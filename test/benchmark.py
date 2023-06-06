import time
from progress.bar import Bar

def sql(connection, query: str, n=10):
    bar = Bar("PSQL\t", max=n)

    result = []

    cursor = connection.cursor()
    cursor.execute(query)

    for _ in range(n):
        start = time.perf_counter()

        cursor.execute(query)

        end = time.perf_counter()

        execution_time = round((end - start) * 1000, 4)

        result.append(execution_time)

        bar.next()

    bar.finish()
    cursor.close()
    return result[1:]


def neo4j(session, query: str, n=10):
    bar = Bar("NEO4J\t", max=n)

    result = []

    session.run(query)

    for _ in range(n):
        start = time.perf_counter()

        session.run(query)

        end = time.perf_counter()

        execution_time = round((end - start) * 1000, 4)

        result.append(execution_time)
        bar.next()

    bar.finish()
    session.close()

    return result[1:]
