import petl
from load_dim import load_dim

def dim_jenis_kegiatan(data_mart, operasional):
    print("===DIM JENIS KEGIATAN===")

    jenis_kegiatan = petl.fromdb(operasional, "SELECT * FROM jenis_kegiatan")
    dim_jenis_kegiatan = petl.fromdb(data_mart, "SELECT * FROM dim_jenis_kegiatan")
    jenis_kegiatan = petl.rename(jenis_kegiatan, {"jenis_kegiatan": "nama_jenis_kegiatan"})

    cursor = data_mart.cursor()
    load_dim("dim_jenis_kegiatan", dim_jenis_kegiatan, jenis_kegiatan, "jenis_kegiatan_key", "id_jenis_kegiatan", cursor)