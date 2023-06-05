from py2neo.bulk import create_nodes
import petl

def dim_isu(operasional, graph):
    print("==LOADING ISU==")

    start_index = 0
    end_index = 100_000

    table_isu = petl.fromdb(operasional, "SELECT * FROM isu")
    input_table = petl.rowslice(table_isu, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["Dimensi", "Isu"])
        print(graph.nodes.match("Dimensi", "Isu").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_isu, start_index, end_index)

    graph.run(
        "CREATE RANGE INDEX isuIndex IF NOT EXISTS FOR (isu:Isu) on (isu.id_isu, isu.nama_isu)")
