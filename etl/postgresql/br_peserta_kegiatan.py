import petl


def br_peserta_kegiatan(data_mart, operasional):
    print("===BR PESERTA KEGIATAN===")

    peserta_kegiatan = petl.fromdb(
        operasional, "SELECT * FROM peserta_kegiatan_proyek")

    dim_peserta = petl.fromdb(data_mart, "SELECT * FROM dim_peserta")
    fact_kegiatan = petl.fromdb(data_mart, "SELECT * FROM fact_kegiatan")

    lkp_peserta = petl.dictlookupone(dim_peserta, "id_peserta")
    lkp_kegiatan = petl.dictlookupone(fact_kegiatan, "id_kegiatan")

    br_peserta_kegiatan = petl.convert(peserta_kegiatan, {"id_peserta": lambda id_peserta: lkp_peserta[id_peserta]["peserta_key"],
                                                          "id_kegiatan": lambda id_kegiatan: lkp_kegiatan[id_kegiatan]["kegiatan_key"]})

    br_peserta_kegiatan = petl.rename(br_peserta_kegiatan, {
                                      "id_peserta": "peserta_key", "id_kegiatan": "kegiatan_key"})

    cursor = data_mart.cursor()
    petl.todb(br_peserta_kegiatan, cursor, "br_peserta_kegiatan")
