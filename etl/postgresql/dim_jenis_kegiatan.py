import petl
from load_dim import load_dim

def dim_jenis_kegiatan(data_mart, operasional):
    print("===DIM JENIS KEGIATAN===")

    cursor = data_mart.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.dim_jenis_kegiatan
    (
        jenis_kegiatan_key serial NOT NULL,
        nama_jenis_kegiatan character varying NOT NULL,
        id_jenis_kegiatan integer,
        CONSTRAINT dim_jenis_kegiatan_pkey PRIMARY KEY (jenis_kegiatan_key)
    )
    """)

    jenis_kegiatan = petl.fromdb(operasional, "SELECT * FROM jenis_kegiatan")
    dim_jenis_kegiatan = petl.fromdb(data_mart, "SELECT * FROM dim_jenis_kegiatan")
    jenis_kegiatan = petl.rename(jenis_kegiatan, {"jenis_kegiatan": "nama_jenis_kegiatan"})

    load_dim("dim_jenis_kegiatan", dim_jenis_kegiatan, jenis_kegiatan, "jenis_kegiatan_key", "id_jenis_kegiatan", cursor)