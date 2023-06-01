from py2neo.bulk import create_nodes
import petl


def dim_negara(operasional, graph):
    print("==LOADING NEGARA==")

    start_index = 0
    end_index = 100_000

    table_negara = petl.fromdb(operasional, "SELECT * FROM negara")
    input_table = petl.rowslice(table_negara, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["Dimensi", "Negara"])
        print(graph.nodes.match("Dimensi", "Negara").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_negara, start_index, end_index)

    graph.run(
        "CREATE RANGE INDEX negaraIndex IF NOT EXISTS FOR (negara:Negara) on (negara.id_negara, negara.nama_negara)")
