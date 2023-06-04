from dotenv import load_dotenv
from py2neo.bulk import create_nodes
import petl

load_dotenv()


def dim_penerima_manfaat(operasional, graph):
    print("==LOADING PENERIMA MANFAAT==")

    start_index = 0
    end_index = 100_000

    table_penerima_manfaat = petl.fromdb(
        operasional, "SELECT * FROM penerima_manfaat")
    input_table = petl.rowslice(table_penerima_manfaat, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=[
                     "Dimensi", "PenerimaManfaat"])
        print(graph.nodes.match("Dimensi", "PenerimaManfaat").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(
            table_penerima_manfaat, start_index, end_index)

    graph.run(
        "CREATE RANGE INDEX penerimaManfaatIndex IF NOT EXISTS FOR (penerima:PenerimaManfaat) on (penerima.id_penerima_manfaat, penerima.penerima_manfaat)")
