import petl
from load_dim import load_dim


def dim_kab_kota(data_mart, operasional):
    print("===DIM KAB KOTA===")

    kab_kota = petl.fromdb(operasional, "SELECT * FROM kabupaten_kota")
    dim_kota = petl.fromdb(data_mart, "SELECT * FROM dim_kabupaten_kota")
    dim_provinsi = petl.fromdb(data_mart, "SELECT * FROM dim_provinsi")

    lkp_dim_provinsi = petl.dictlookupone(dim_provinsi, "id_provinsi")
    kab_kota = petl.convert(kab_kota, {
        "id_provinsi": lambda id_provinsi: lkp_dim_provinsi[id_provinsi]["provinsi_key"]})
    kab_kota = petl.rename(kab_kota, {"id_provinsi": "provinsi_key"})
    kab_kota = petl.cutout(kab_kota, "kode_bps")

    cursor = data_mart.cursor()
    # petl.todb(kab_kota, cursor, "dim_kabupaten_kota")
    load_dim("dim_kabupaten_kota", dim_kota, kab_kota,
             "kab_kota_key", "id_kab_kota", cursor)
