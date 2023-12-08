import petl
from load_dim import load_dim

def dim_desa_kelurahan(data_mart, operasional):
    print("===DIM DESA KELURAHAN===")

    cursor = data_mart.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.dim_desa_kelurahan
    (
        desa_kel_key serial NOT NULL,
        kecamatan_key integer NOT NULL,
        nama_desa_kel character varying NOT NULL,
        id_desa_kel integer,
        CONSTRAINT dim_desa_kelurahan_pkey PRIMARY KEY (desa_kel_key),
        CONSTRAINT desa_kelurahan FOREIGN KEY (kecamatan_key)
            REFERENCES public.dim_kecamatan (kecamatan_key) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
    )
    """)

    desa = petl.fromdb(operasional, "SELECT * FROM desa_kelurahan")
    dim_desa_kel = petl.fromdb(data_mart, "SELECT * FROM dim_desa_kelurahan")
    dim_kecamatan = petl.fromdb(data_mart, "SELECT * FROM dim_kecamatan")

    lkp_dim_kecamatan = petl.dictlookupone(dim_kecamatan, "id_kecamatan")
    desa = petl.convert(desa, {
        "id_kecamatan": lambda id_kecamatan: lkp_dim_kecamatan[id_kecamatan]["kecamatan_key"]})
    desa = petl.rename(desa, {"id_kecamatan": "kecamatan_key"})
    desa = petl.cutout(desa, "kode_bps")

    load_dim("dim_desa_kelurahan", dim_desa_kel, desa,
             "desa_kel_key", "id_desa_kel", cursor)
