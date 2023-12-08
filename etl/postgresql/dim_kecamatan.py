import petl
from load_dim import load_dim

def dim_kecamatan(data_mart, operasional):
    print("===DIM KECAMATAN===")

    cursor = data_mart.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.dim_kecamatan
    (
        kecamatan_key serial NOT NULL,
        kab_kota_key integer NOT NULL,
        nama_kecamatan character varying NOT NULL,
        id_kecamatan integer,
        CONSTRAINT dim_kecamatan_pkey PRIMARY KEY (kecamatan_key),
        CONSTRAINT kabupaten_kota FOREIGN KEY (kab_kota_key)
            REFERENCES public.dim_kabupaten_kota (kab_kota_key) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
    )
    """)

    kecamatan = petl.fromdb(operasional, "SELECT * FROM kecamatan")
    dim_kecamatan = petl.fromdb(data_mart, "SELECT * FROM dim_kecamatan")
    dim_kab_kota = petl.fromdb(data_mart, "SELECT * FROM dim_kabupaten_kota")

    lkp_dim_kab_kota = petl.dictlookupone(dim_kab_kota, "id_kab_kota")
    kecamatan = petl.convert(kecamatan, {
                             "id_kab_kota": lambda id_kab_kota: lkp_dim_kab_kota[id_kab_kota]["kab_kota_key"]})
    kecamatan = petl.rename(kecamatan, {"id_kab_kota": "kab_kota_key"})
    kecamatan = petl.cutout(kecamatan, "kode_bps")

    load_dim("dim_kecamatan", dim_kecamatan, kecamatan,
             "kecamatan_key", "id_kecamatan", cursor)
