import petl

def dim_jenis_kegiatan(data_mart, operasional):
    print("===DIM JENIS KEGIATAN===")

    jenis_kegiatan = petl.fromdb(operasional, "SELECT * FROM jenis_kegiatan")
    jenis_kegiatan = petl.rename(jenis_kegiatan, {"jenis_kegiatan": "nama_jenis_kegiatan"})

    cursor = data_mart.cursor()
    petl.todb(jenis_kegiatan, cursor, "dim_jenis_kegiatan")