import petl

def dim_desa_kelurahan(data_mart, operasional):
    print("===DIM DESA KELURAHAN===")

    desa = petl.fromdb(operasional, "SELECT * FROM desa_kelurahan")
    dim_kecamatan = petl.fromdb(data_mart, "SELECT * FROM dim_kecamatan")

    lkp_dim_kecamatan = petl.dictlookupone(dim_kecamatan, "id_kecamatan")
    dim_desa = petl.convert(desa, {
                            "id_kecamatan": lambda id_kecamatan: lkp_dim_kecamatan[id_kecamatan]["kecamatan_key"]})
    dim_desa = petl.rename(dim_desa, {"id_kecamatan": "kecamatan_key"})
    dim_desa = petl.cutout(dim_desa, "kode_bps")

    cursor = data_mart.cursor()
    petl.todb(dim_desa, cursor, "dim_desa_kelurahan")
