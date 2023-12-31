import petl
from py2neo.bulk import create_nodes

def fact_kegiatan(operasional, graph):
    print("==LOADING FACT KEGIATAN==")

    start_index = 0
    end_index = 100_000

    table_kegiatan = petl.fromdb(operasional, "SELECT * FROM kegiatan")

    peserta_kegiatan = petl.fromdb(
        operasional, "SELECT * FROM peserta_kegiatan_proyek")

    lkp_peserta_kegiatan = petl.dictlookup(peserta_kegiatan, "id_kegiatan")

    table_kegiatan = petl.addfield(table_kegiatan,
                                   field="jumlah_peserta",
                                   value=lambda row: len(
                                       lkp_peserta_kegiatan[row["id_kegiatan"]])
                                   )

    input_table = petl.rowslice(table_kegiatan, start_index, end_index)

    while petl.nrows(input_table) > 0:
        input_table = petl.dicts(input_table)

        create_nodes(graph.auto(), input_table, labels=["Fact", "Kegiatan"])
        print(graph.nodes.match("Fact", "Kegiatan").count())

        start_index = end_index
        end_index += 100_000
        input_table = petl.rowslice(table_kegiatan, start_index, end_index)

    graph.run(
        "CREATE RANGE INDEX kegiatanIndex IF NOT EXISTS FOR (kegiatan:Kegiatan) on (kegiatan.id_kegiatan)")

    graph.run(
        "MATCH (kegiatan:Kegiatan), (proyek:Proyek) WHERE kegiatan.id_proyek = proyek.id_proyek CREATE (kegiatan)-[r:KEGIATAN_DARI]->(proyek)")

    graph.run(
        "MATCH (kegiatan:Kegiatan), (waktu:Waktu) WHERE kegiatan.tanggal_rencana = waktu.tanggal CREATE (kegiatan)-[r:DIRENCANAKAN_PADA]->(waktu)")

    graph.run(
        "MATCH (kegiatan:Kegiatan), (waktu:Waktu) WHERE kegiatan.tanggal_pelaksanaan = waktu.tanggal CREATE (kegiatan)-[r:DILAKSANAKAN_PADA]->(waktu)")

    graph.run(
        "MATCH (kegiatan:Kegiatan), (lembaga:LembagaPelaksana) WHERE kegiatan.id_lembaga_pelaksana = lembaga.id_lembaga_pelaksana CREATE (lembaga)-[r:MELAKSANAKAN]->(kegiatan)")

    graph.run(
        "MATCH (kegiatan:Kegiatan), (penerima:PenerimaManfaat) WHERE kegiatan.id_penerima_manfaat = penerima.id_penerima_manfaat CREATE (kegiatan)-[r:MENARGETKAN]->(penerima)")

    graph.run(
        "MATCH (kegiatan:Kegiatan), (jenis:JenisKegiatan) WHERE kegiatan.id_jenis_kegiatan = jenis.id_jenis_kegiatan CREATE (kegiatan)-[r:BERJENIS]->(jenis)")

    # RELASI LOKASI
    graph.run(
        "MATCH (kegiatan:Kegiatan), (kecamatan:Kecamatan) WHERE kegiatan.id_kecamatan = kecamatan.id_kecamatan CREATE (kegiatan)-[r:BERADA]->(kecamatan)")

    graph.run(
        "MATCH (kegiatan:Kegiatan), (kota:Kota|Kabupaten) WHERE kegiatan.id_kota = kota.id_kab_kota CREATE (kegiatan)-[r:BERADA]->(kota)")

    graph.run(
        "MATCH (kegiatan:Kegiatan), (desa:Desa|Kelurahan) WHERE kegiatan.id_desa = desa.id_desa_kel CREATE (kegiatan)-[r:BERADA]->(desa)")

