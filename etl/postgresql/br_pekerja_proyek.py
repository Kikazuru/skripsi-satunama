import petl

def br_pekerja_proyek(data_mart, operasional):
    print("===DIM BR PEKERJA PROYEK===")

    pekerja_proyek = petl.fromdb(operasional, "SELECT * FROM pekerja_proyek")
    jabatan = petl.fromdb(operasional, "SELECT * FROM jabatan_proyek")

    dim_pekerja = petl.fromdb(data_mart, "SELECT * FROM dim_pekerja")
    fact_proyek = petl.fromdb(data_mart, "SELECT * FROM fact_proyek")

    lkp_pekerja = petl.dictlookupone(dim_pekerja, "id_pekerja")
    lkp_jabatan = petl.dictlookupone(jabatan, "id_jabatan")
    lkp_proyek = petl.dictlookupone(fact_proyek, "id_proyek")

    br_pekerja_proyek = petl.convert(pekerja_proyek, {"id_pekerja": lambda id_pekerja: lkp_pekerja[id_pekerja]["pekerja_key"],
                                                      "id_proyek": lambda id_proyek: lkp_proyek[id_proyek]["proyek_key"],
                                                      "id_jabatan_proyek": lambda id_jabatan: lkp_jabatan[id_jabatan]["nama_jabatan"],
                                                      })

    br_pekerja_proyek = petl.rename(
        br_pekerja_proyek, {"id_pekerja": "pekerja_key", "id_proyek": "proyek_key", "id_jabatan_proyek": "jabatan_proyek"})

    cursor = data_mart.cursor()
    petl.todb(br_pekerja_proyek, cursor, "br_pekerja_proyek")
