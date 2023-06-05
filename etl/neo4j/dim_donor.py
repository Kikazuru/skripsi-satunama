from py2neo.bulk import create_nodes
import petl

def dim_donor(operasional, graph):
    print("==LOADING DONOR==")

    start_index = 0
    end_index = 100_000

    table_donor = petl.fromdb(
        operasional, "select * from donor")
    input_table = petl.rowslice(table_donor, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["Dimensi", "Donor"])
        print(graph.nodes.match("Dimensi", "Donor").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_donor, start_index, end_index)

    graph.run(
        "MATCH (negara:Negara), (donor:Donor) WHERE negara.id_negara = donor.id_negara CREATE (donor)-[r:BERLOKASI_DI]->(negara)")

    graph.run(
        "CREATE RANGE INDEX donorIndex IF NOT EXISTS FOR (donor:Donor) on (donor.id_donor, donor.nama_donor)")
