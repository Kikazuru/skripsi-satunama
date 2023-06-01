from py2neo.bulk import create_nodes
import petl

def dim_jenis_kegiatan(operasional, graph):
    print("==LOADING JENIS KEGIATAN==")

    start_index = 0
    end_index = 100_000

    table_jenis_kegiatan = petl.fromdb(
        operasional, "SELECT * FROM jenis_kegiatan")
    input_table = petl.rowslice(table_jenis_kegiatan, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=[
                     "Dimensi", "JenisKegiatan"])
        print(graph.nodes.match("Dimensi", "JenisKegiatan").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(
            table_jenis_kegiatan, start_index, end_index)

    graph.run(
        "CREATE RANGE INDEX jenisKegiatanIndex IF NOT EXISTS FOR (jenis:JenisKegiatan) on (jenis.id_jenis_kegiatan, jenis.nama_jenis_kegiatan)")
