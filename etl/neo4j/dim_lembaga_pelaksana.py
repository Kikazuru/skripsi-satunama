from py2neo.bulk import create_nodes
import petl

def dim_lembaga_pelaksana(operasional, graph):
    print("==LOADING LEMBAGA PELAKSANA==")

    start_index = 0
    end_index = 100_000

    table_lembaga = petl.fromdb(
        operasional, "select * from lembaga_pelaksana")
    input_table = petl.rowslice(table_lembaga, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=[
                     "Dimensi", "LembagaPelaksana"])
        print(graph.nodes.match("Dimensi", "LembagaPelaksana").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_lembaga, start_index, end_index)

    graph.run(
        "CREATE RANGE INDEX lembagaPelaksanaIndex IF NOT EXISTS FOR (lembaga:LembagaPelaksana) on (lembaga.id_lembaga_pelaksana, lembaga.nama_lembaga)")

    graph.run(
        "MATCH (kota:Kota|Kabupaten), (lembaga:LembagaPelaksana) WHERE lembaga.id_kota = kota.id_kab_kota CREATE (lembaga)-[r:BERADA]->(kota)")
