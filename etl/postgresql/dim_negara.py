import petl
from load_dim import load_dim


def dim_negara(data_mart, operasional):
    print("===DIM NEGARA===")

    cursor = data_mart.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.dim_negara
    (
        negara_key serial NOT NULL,
        nama_negara character varying NOT NULL,
        id_negara integer,
        CONSTRAINT dim_negara_pkey PRIMARY KEY (negara_key)
    )
    """)

    negara = petl.fromdb(operasional, "SELECT * FROM negara")
    dim_negara = petl.fromdb(data_mart, "SELECT * FROM dim_negara")
 
    load_dim("dim_negara", dim_negara, negara,
             "negara_key", "id_negara", cursor)
