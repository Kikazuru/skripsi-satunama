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

table_peserta = petl.fromdb(operasional, "SELECT * FROM peserta")
table_peserta = petl.rowslice(table_peserta, 3_000_000)
petl.tocsv(table_peserta, full_path + "/peserta.csv")

uri = "neo4j://localhost:7687/"
with GraphDatabase.driver(uri, auth=("neo4j", "@Harris99")) as driver:
    session = driver.session(database="datamart")
    file_url = "file:///" + str(full_path).replace("\\", "/") + "/peserta.csv"

    try:
        query = f"""
        LOAD CSV WITH HEADERS FROM '{file_url}' AS row
        CREATE (:Peserta {{id_peserta: row.id_peserta, nama_peserta: row.nama_peserta,  tanggal_lahir: row.tanggal_lahir}})
        """
        result = session.run(query)
        pass
    finally:
        session.close()
