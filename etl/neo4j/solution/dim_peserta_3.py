from dotenv import load_dotenv
from py2neo import Graph
from py2neo.bulk import create_nodes
import petl
import psycopg2
import os

load_dotenv()

operasional = psycopg2.connect(
    f'host={os.getenv("DBHOST_OP")} dbname={os.getenv("DBNAME_OP")} user={os.getenv("DBUSER_OP")} password={os.getenv("DBPASS_OP")}')

table_peserta = petl.fromdb(operasional, "SELECT * FROM peserta")
table_peserta = petl.rowslice(table_peserta, 400_000)
table_peserta = petl.cutout(table_peserta, "jenis_kelamin", "tanggal_lahir")
table_peserta = petl.dicts(table_peserta)

graph = Graph("neo4j://localhost:7687/",
              auth=("neo4j", "@Harris99"), name="datamart")
create_nodes(graph.auto(), table_peserta, ("Peserta", "nama_peserta"))
print(graph.nodes.match("Peserta").count())
