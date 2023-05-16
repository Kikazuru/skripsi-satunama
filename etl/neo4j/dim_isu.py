from dotenv import load_dotenv
from neo4j import GraphDatabase
import petl
import psycopg2
import os

absolute_path = os.path.dirname(__file__)
relative_path = "csv"
full_path = os.path.join(absolute_path, relative_path)

load_dotenv()

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

table_isu = petl.fromdb(operasional, "SELECT * FROM isu")
petl.tocsv(table_isu, full_path + "/isu.csv")

uri = "neo4j://localhost:7687/"
with GraphDatabase.driver(uri, auth=("neo4j", "@Harris99")) as driver:
    session = driver.session(database="datamart")
    file_url = "file:///" + str(full_path).replace("\\", "/") + "/isu.csv"

    try:
        query = f"""
        LOAD CSV WITH HEADERS FROM '{file_url}' AS row
        MERGE (:isu {{id_isu: row.id_isu, nama_isu: row.nama_isu}})
        """
        result = session.run(query)
        pass
    finally:
        session.close()
