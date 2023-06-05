from py2neo.bulk import create_nodes
import petl

def dim_kecamatan(operasional, graph):
    print("==LOADING KECAMATAN==")

    start_index = 0
    end_index = 100_000

    table_kecamatan = petl.fromdb(operasional, "SELECT * FROM kecamatan")
    table_kecamatan = petl.cutout(table_kecamatan, "kode_bps")
    input_table = petl.rowslice(table_kecamatan, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table,
                     labels=["Dimensi", "Kecamatan"])
        print(graph.nodes.match("Dimensi", "Kecamatan").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_kecamatan, start_index, end_index)

    graph.run(
        "CREATE RANGE INDEX kecamatanIndex IF NOT EXISTS FOR (kecamatan:Kecamatan) on (kecamatan.id_kecamatan, kecamatan.nama_kecamatan)")

    graph.run(
        "MATCH (kecamatan:Kecamatan), (kota:Kota|Kabupaten) WHERE kecamatan.id_kab_kota = kota.id_kab_kota CREATE (kecamatan)-[r:KECAMATAN_DARI]->(kota) return kota, kecamatan")
