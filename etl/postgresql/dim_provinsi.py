import petl
from load_dim import load_dim


def dim_provinsi(data_mart, operasional):
    print("===DIM PROVINSI===")
    cursor = data_mart.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.dim_provinsi
    (
        provinsi_key serial NOT NULL ,
        negara_key integer NOT NULL,
        nama_provinsi character varying NOT NULL,
        id_provinsi integer,
        CONSTRAINT dim_provinsi_pkey PRIMARY KEY (provinsi_key),
        CONSTRAINT negara FOREIGN KEY (negara_key)
            REFERENCES public.dim_negara (negara_key) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
    )
    """)
    
    provinsi = petl.fromdb(operasional, "SELECT * FROM provinsi")
    dim_provinsi = petl.fromdb(data_mart, "SELECT * FROM dim_provinsi")
    dim_negara = petl.fromdb(data_mart, "SELECT * FROM dim_negara")

    lkp_dim_negara = petl.dictlookupone(dim_negara, "id_negara")
    provinsi = petl.convert(
        provinsi, {"id_negara": lambda id_negara: lkp_dim_negara[id_negara]["negara_key"]})
    provinsi = petl.rename(provinsi, {"id_negara": "negara_key"})
    provinsi = petl.cutout(provinsi, "kode_bps")

    load_dim("dim_provinsi", dim_provinsi, provinsi,
             "provinsi_key", "id_provinsi", cursor)
