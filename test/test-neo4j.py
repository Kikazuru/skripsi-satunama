from neo4j import GraphDatabase
import time

uri = "neo4j://localhost:7687/"
with GraphDatabase.driver(uri, auth=("neo4j", "@Harris99")) as driver:
    session = driver.session(database="datamart")

    print("start")
    # get the start time
    st = time.time()

    result = session.run("MATCH (n:Peserta) return n")

    # get the end time
    et = time.time()
    print("done")

    # get the execution time
    elapsed_time = et - st
    print('Execution time:', round(elapsed_time, 4), 'seconds')
