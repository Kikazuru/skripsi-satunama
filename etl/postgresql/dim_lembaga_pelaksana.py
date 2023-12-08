import petl
from load_dim import load_dim

def dim_lembaga_pelaksana(data_mart, operasional):
    print("===DIM LEMBAGA PELAKSANA===")

    lembaga_pelaksana = petl.fromdb(
        operasional, "SELECT * FROM lembaga_pelaksana")
    dim_lembaga_pelaksana = petl.fromdb(
        data_mart, "SELECT * FROM dim_lembaga_pelaksana")

    dim_kota = petl.fromdb(data_mart, "SELECT * FROM dim_kabupaten_kota")
    lkp_kota = petl.dictlookupone(dim_kota, "id_kab_kota")

    lembaga_pelaksana = petl.convert(
        lembaga_pelaksana, {"id_kota": lambda id_kota: lkp_kota[id_kota]["kab_kota_key"]})
    lembaga_pelaksana = petl.rename(
        lembaga_pelaksana, {"id_kota": "kota_key"})

    cursor = data_mart.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.dim_lembaga_pelaksana
    (
        lembaga_key serial NOT NULL,
        nama_lembaga character varying NOT NULL,
        kota_key integer NOT NULL,
        id_lembaga_pelaksana integer NOT NULL,
        singkatan character varying,
        CONSTRAINT dim_lembaga_pelaksana_pkey PRIMARY KEY (lembaga_key),
        CONSTRAINT kota FOREIGN KEY (kota_key)
            REFERENCES public.dim_kabupaten_kota (kab_kota_key) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
    )
    """)

    load_dim("dim_lembaga_pelaksana", dim_lembaga_pelaksana,
             lembaga_pelaksana, "lembaga_key", "id_lembaga_pelaksana", cursor)
