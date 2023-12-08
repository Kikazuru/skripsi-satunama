import petl
from load_dim import load_dim


def dim_penerima_manfaat(data_mart, operasional):
    print("===DIM PENERIMAAN MANFAAT===")

    cursor = data_mart.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.dim_penerima_manfaat
    (
        penerima_manfaat_key serial NOT NULL,
        penerima_manfaat character varying NOT NULL,
        id_penerima_manfaat integer,
        CONSTRAINT dim_penerima_manfaat_pkey PRIMARY KEY (penerima_manfaat_key)
    )
    """)

    penerima_manfaat = petl.fromdb(
        operasional, "SELECT * FROM penerima_manfaat")
    dim_penerima_manfaat = petl.fromdb(
        data_mart, "SELECT * FROM dim_penerima_manfaat")

    load_dim("dim_penerima_manfaat", dim_penerima_manfaat, penerima_manfaat,
             "penerima_manfaat_key", "id_penerima_manfaat", cursor)
