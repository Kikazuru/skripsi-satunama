import petl
from load_dim import load_dim


def dim_peserta(data_mart, operasional):
    print("===DIM PESERTA===")

    cursor = data_mart.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.dim_peserta
    (
        peserta_key serial NOT NULL,
        nama_peserta character varying NOT NULL,
        jenis_kelamin character varying,
        tanggal_lahir date,
        id_peserta integer,
        CONSTRAINT dim_peserta_pkey PRIMARY KEY (peserta_key)
    )
    """)

    peserta = petl.fromdb(operasional, "SELECT * FROM peserta")
    dim_peserta = petl.fromdb(data_mart, "SELECT * FROM dim_peserta")

    load_dim("dim_peserta", dim_peserta, peserta,
             "peserta_key", "id_peserta", cursor)
