from py2neo.bulk import create_nodes
import petl

def dim_desa(operasional, graph):
    print("==LOADING DESA==")

    start_index = 0
    end_index = 100_000

    table_desa = petl.fromdb(operasional, "SELECT * FROM desa_kelurahan")
    table_desa = petl.cutout(table_desa, "kode_bps")
    input_table = petl.rowslice(table_desa, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=[
                     "Dimensi", "Desa", "Kelurahan"])
        print(graph.nodes.match("Dimensi", "Desa", "Kelurahan").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_desa, start_index, end_index)

    graph.run(
        "CREATE RANGE INDEX desaIndex IF NOT EXISTS FOR (desa:Desa) on (desa.id_desa_kel, desa.nama_desa_kel)")

    graph.run(
        "CREATE RANGE INDEX desaIndex IF NOT EXISTS FOR (kel:Kelurahan) on (kel.id_desa_kel, kel.nama_desa_kel)")

    graph.run(
        "MATCH (kecamatan:Kecamatan), (desa:Desa) WHERE kecamatan.id_kecamatan = desa.id_kecamatan CREATE (desa)-[r:DESA_DARI]->(kecamatan)")
