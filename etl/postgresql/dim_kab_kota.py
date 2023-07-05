import petl

def dim_kab_kota(data_mart, operasional):
    print("===DIM KAB KOTA===")

    kab_kota = petl.fromdb(operasional, "SELECT * FROM kabupaten_kota")
    dim_provinsi = petl.fromdb(data_mart, "SELECT * FROM dim_provinsi")

    lkp_dim_provinsi = petl.dictlookupone(dim_provinsi, "id_provinsi")
    dim_kab_kota = petl.convert(kab_kota, {"id_provinsi" : lambda id_provinsi: lkp_dim_provinsi[id_provinsi]["provinsi_key"]})
    dim_kab_kota = petl.rename(dim_kab_kota, {"id_provinsi": "provinsi_key"})
    dim_kab_kota = petl.cutout(dim_kab_kota, "kode_bps")

    cursor = data_mart.cursor()
    petl.todb(dim_kab_kota, cursor, "dim_kabupaten_kota")