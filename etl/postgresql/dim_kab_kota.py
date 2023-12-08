import petl
from load_dim import load_dim


def dim_kab_kota(data_mart, operasional):
    print("===DIM KAB KOTA===")

    cursor = data_mart.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.dim_kabupaten_kota
    (
        kab_kota_key serial NOT NULL,
        provinsi_key integer NOT NULL,
        nama_kab_kota character varying NOT NULL,
        id_kab_kota integer,
        CONSTRAINT dim_kabupaten_kota_pkey PRIMARY KEY (kab_kota_key),
        CONSTRAINT provinsi FOREIGN KEY (provinsi_key)
            REFERENCES public.dim_provinsi (provinsi_key) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
    )
    """)

    kab_kota = petl.fromdb(operasional, "SELECT * FROM kabupaten_kota")
    dim_kota = petl.fromdb(data_mart, "SELECT * FROM dim_kabupaten_kota")
    dim_provinsi = petl.fromdb(data_mart, "SELECT * FROM dim_provinsi")

    lkp_dim_provinsi = petl.dictlookupone(dim_provinsi, "id_provinsi")
    kab_kota = petl.convert(kab_kota, {
        "id_provinsi": lambda id_provinsi: lkp_dim_provinsi[id_provinsi]["provinsi_key"]})
    kab_kota = petl.rename(kab_kota, {"id_provinsi": "provinsi_key"})
    kab_kota = petl.cutout(kab_kota, "kode_bps")

    load_dim("dim_kabupaten_kota", dim_kota, kab_kota,
             "kab_kota_key", "id_kab_kota", cursor)
