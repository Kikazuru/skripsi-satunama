import petl
from load_dim import load_dim

def dim_donor(data_mart, operasional):
    print("===DIM DONOR===")

    cursor = data_mart.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.dim_donor
    (
        donor_key serial NOT NULL,
        nama_donor character varying NOT NULL,
        negara_key integer,
        id_donor integer,
        singkatan character varying,
        CONSTRAINT dim_donor_pkey PRIMARY KEY (donor_key),
        CONSTRAINT negara FOREIGN KEY (negara_key)
            REFERENCES public.dim_negara (negara_key) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
    )
    """)

    donor = petl.fromdb(operasional, "SELECT * FROM donor")
    dim_donor = petl.fromdb(data_mart, "SELECT * FROM dim_donor")
    dim_negara = petl.fromdb(data_mart, "SELECT * FROM dim_negara")
    lkp_negara = petl.dictlookupone(dim_negara, "id_negara")

    donor = petl.convert(
        donor, {"id_negara": lambda id_negara: lkp_negara[id_negara]["negara_key"]})
    donor = petl.rename(donor, {"id_negara": "negara_key"})

    load_dim("dim_donor", dim_donor, donor, "donor_key", "id_donor", cursor)
