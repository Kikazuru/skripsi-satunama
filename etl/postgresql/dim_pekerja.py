import petl
from load_dim import load_dim


def dim_pekerja(data_mart, operasional):
    print("===DIM PEKERJA===")

    cursor = data_mart.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.dim_pekerja
    (
        pekerja_key serial NOT NULL,
        nama_pekerja character varying NOT NULL,
        id_pekerja integer,
        id_karyawan integer,
        CONSTRAINT dim_pekerja_pkey PRIMARY KEY (pekerja_key)
    )
    """)

    pekerja = petl.fromdb(operasional, "SELECT * FROM pekerja")
    dim_pekerja = petl.fromdb(data_mart, "SELECT * FROM dim_pekerja")

    load_dim("dim_pekerja", dim_pekerja, pekerja,
             "pekerja_key", "id_pekerja", cursor)
