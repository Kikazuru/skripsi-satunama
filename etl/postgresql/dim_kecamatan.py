import petl

def dim_kecamatan(data_mart, operasional):
    print("===DIM KECAMATAN===")

    kecamatan = petl.fromdb(operasional, "SELECT * FROM kecamatan")
    dim_kab_kota = petl.fromdb(data_mart, "SELECT * FROM dim_kabupaten_kota")

    lkp_dim_kab_kota = petl.dictlookupone(dim_kab_kota, "id_kab_kota")
    dim_kecamatan = petl.convert(kecamatan, {"id_kab_kota" : lambda id_kab_kota: lkp_dim_kab_kota[id_kab_kota]["kab_kota_key"]})
    dim_kecamatan = petl.rename(dim_kecamatan, {"id_kab_kota": "kab_kota_key"})
    dim_kecamatan = petl.cutout(dim_kecamatan, "kode_bps")

    cursor = data_mart.cursor()
    petl.todb(dim_kecamatan, cursor, "dim_kecamatan")