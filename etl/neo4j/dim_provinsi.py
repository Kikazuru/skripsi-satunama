from dotenv import load_dotenv
from py2neo.bulk import create_nodes
import petl

load_dotenv()


def dim_provinsi(operasional, graph):
    print("==LOADING PROVINSI==")

    start_index = 0
    end_index = 100_000

    table_provinsi = petl.fromdb(operasional, "SELECT * FROM provinsi")
    input_table = petl.rowslice(table_provinsi, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["Dimensi", "Provinsi"])
        print(graph.nodes.match("Dimensi", "Provinsi").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_provinsi, start_index, end_index)

    graph.run(
        "CREATE RANGE INDEX provinsiIndex IF NOT EXISTS FOR (provinsi:Provinsi) on (provinsi.id_provinsi, provinsi.nama_provinsi)")

    graph.run(
        "MATCH (provinsi:Provinsi), (negara:Negara) WHERE provinsi.id_negara = negara.id_negara CREATE (provinsi)-[r:PROVINSI_DARI]->(negara)")
