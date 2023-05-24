from dotenv import load_dotenv
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
import petl
import psycopg2
import os

load_dotenv()

start_index = 0
end_index = 100_000

operasional = psycopg2.connect(
    f'dbname={os.getenv("DB_NAME")} user={os.getenv("DB_USER")} password={os.getenv("DB_PASS")}')

table_kab_kota = petl.fromdb(operasional, "SELECT * FROM kabupaten_kota")
input_table = petl.rowslice(table_kab_kota, start_index, end_index)

graph = Graph("neo4j://localhost:7687/",
              auth=("neo4j", "@Harris99"), name="datamart")

while petl.nrows(input_table) > 0:
    input_table = petl.dicts(input_table)

    create_nodes(graph.auto(), input_table, labels=["DimKabKota"])
    print(graph.nodes.match("DimKabKota").count())

    relation = [
        ((row["id_kab_kota"],), {}, row["id_provinsi"]) for row in input_table
    ]
    create_relationships(graph.auto(),
                         data=relation,
                         rel_type="KOTA_DARI",
                         start_node_key=("DimKabKota", "id_kab_kota"),
                         end_node_key=("DimProvinsi", "id_provinsi"))

    start_index = end_index
    end_index += 100_000
    input_table = petl.rowslice(table_kab_kota, start_index, end_index)
