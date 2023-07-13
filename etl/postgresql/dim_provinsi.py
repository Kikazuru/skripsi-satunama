import petl
from load_dim import load_dim


def dim_provinsi(data_mart, operasional):
    print("===DIM PROVINSI===")
    provinsi = petl.fromdb(operasional, "SELECT * FROM provinsi")
    dim_provinsi = petl.fromdb(data_mart, "SELECT * FROM dim_provinsi")
    dim_negara = petl.fromdb(data_mart, "SELECT * FROM dim_negara")

    lkp_dim_negara = petl.dictlookupone(dim_negara, "id_negara")
    provinsi = petl.convert(
        provinsi, {"id_negara": lambda id_negara: lkp_dim_negara[id_negara]["negara_key"]})
    provinsi = petl.rename(provinsi, {"id_negara": "negara_key"})
    provinsi = petl.cutout(provinsi, "kode_bps")

    cursor = data_mart.cursor()
    load_dim("dim_provinsi", dim_provinsi, provinsi,
             "provinsi_key", "id_provinsi", cursor)
