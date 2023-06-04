from dotenv import load_dotenv
from py2neo.bulk import create_nodes
import petl

load_dotenv()


def dim_peserta(operasional, graph):
    print("==LOADING PESERTA==")

    start_index = 0
    end_index = 100_000

    table_peserta = petl.fromdb(operasional, "SELECT * FROM peserta")
    input_table = petl.rowslice(table_peserta, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["Dimensi", "Peserta"])
        print(graph.nodes.match("Dimensi", "Peserta").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_peserta, start_index, end_index)
    
    graph.run(
            "CREATE RANGE INDEX pesertaIndex IF NOT EXISTS FOR (peserta:Peserta) on (peserta.id_peserta)")