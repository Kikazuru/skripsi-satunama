import petl

def dim_provinsi(data_mart, operasional):
    print("===DIM PROVINSI===")
    provinsi = petl.fromdb(operasional, "SELECT * FROM provinsi")
    dim_negara = petl.fromdb(data_mart, "SELECT * FROM dim_negara")

    lkp_dim_negara = petl.dictlookupone(dim_negara, "id_negara")
    dim_provinsi = petl.convert(provinsi, {"id_negara" : lambda id_negara: lkp_dim_negara[id_negara]["negara_key"]})
    dim_provinsi = petl.rename(dim_provinsi, {"id_negara": "negara_key"})
    dim_provinsi = petl.cutout(dim_provinsi, "kode_bps")

    cursor = data_mart.cursor()
    petl.todb(dim_provinsi, cursor, "dim_provinsi")