import petl
from load_dim import load_dim


def dim_isu(data_mart, operasional):
    print("===DIM ISU===")

    cursor = data_mart.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.dim_isu
    (
        isu_key serial NOT NULL,
        nama_isu character varying NOT NULL,
        id_isu integer,
        CONSTRAINT dim_isu_pkey PRIMARY KEY (isu_key)
    )
    """)

    isu = petl.fromdb(operasional, "SELECT * FROM isu")
    dim_isu = petl.fromdb(data_mart, "SELECT * FROM dim_isu") 

    load_dim("dim_isu", dim_isu, isu, "isu_key", "id_isu", cursor)
