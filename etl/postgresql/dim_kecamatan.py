import petl
from load_dim import load_dim


def dim_kecamatan(data_mart, operasional):
    print("===DIM KECAMATAN===")

    kecamatan = petl.fromdb(operasional, "SELECT * FROM kecamatan")
    dim_kecamatan = petl.fromdb(data_mart, "SELECT * FROM dim_kecamatan")
    dim_kab_kota = petl.fromdb(data_mart, "SELECT * FROM dim_kabupaten_kota")

    lkp_dim_kab_kota = petl.dictlookupone(dim_kab_kota, "id_kab_kota")
    kecamatan = petl.convert(kecamatan, {
                             "id_kab_kota": lambda id_kab_kota: lkp_dim_kab_kota[id_kab_kota]["kab_kota_key"]})
    kecamatan = petl.rename(kecamatan, {"id_kab_kota": "kab_kota_key"})
    kecamatan = petl.cutout(kecamatan, "kode_bps")

    cursor = data_mart.cursor()
    # petl.todb(kecamatan, cursor, "dim_kecamatan")
    load_dim("dim_kecamatan", dim_kecamatan, kecamatan,
             "kecamatan_key", "id_kecamatan", cursor)
