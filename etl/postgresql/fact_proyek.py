import petl

def fact_proyek(data_mart, operasional):
    print("===FACT PROYEK===")
    proyek = petl.fromdb(operasional, "SELECT * FROM proyek")

    dim_isu = petl.fromdb(data_mart, "SELECT * FROM dim_isu")
    dim_donor = petl.fromdb(data_mart, "SELECT * FROM dim_donor")
    dim_waktu = petl.fromdb(data_mart, "SELECT * FROM dim_waktu")
    dim_negara = petl.fromdb(data_mart, "SELECT * FROM dim_negara")
    dim_provinsi = petl.fromdb(data_mart, "SELECT * FROM dim_provinsi")
    dim_kota = petl.fromdb(data_mart, "SELECT * FROM dim_kabupaten_kota")

    lkp_dim_isu = petl.dictlookupone(dim_isu, "id_isu")
    lkp_dim_waktu = petl.dictlookupone(dim_waktu, "tanggal")
    lkp_dim_donor = petl.dictlookupone(dim_donor, "id_donor")
    lkp_dim_negara = petl.dictlookupone(dim_negara, "id_negara")
    lkp_dim_provinsi = petl.dictlookupone(dim_provinsi, "id_provinsi")
    lkp_dim_kota = petl.dictlookupone(dim_kota, "id_kab_kota")

    fact_proyek = petl.convert(proyek, 
                            {
                                "id_donor": lambda id_donor: lkp_dim_donor[id_donor]["donor_key"],
                                "id_isu": lambda id_isu: lkp_dim_isu[id_isu]["isu_key"],
                                "id_negara": lambda id_negara: lkp_dim_negara[id_negara]["negara_key"],
                                "id_provinsi": lambda id_provinsi: lkp_dim_provinsi[id_provinsi]["provinsi_key"],
                                "id_kota": lambda id_kota: lkp_dim_kota[id_kota]["kab_kota_key"],
                                "tanggal_mulai_proyek": lambda tanggal_mulai_proyek: lkp_dim_waktu[tanggal_mulai_proyek]["waktu_key"],
                                "tanggal_selesai_proyek": lambda tanggal_selesai_proyek: lkp_dim_waktu[tanggal_selesai_proyek]["waktu_key"],
                            })

    fact_proyek = petl.rename(fact_proyek, 
                            {
                                "id_donor": "donor_key",
                                "id_isu": "isu_key",
                                "id_negara": "negara_key",
                                "id_provinsi": "provinsi_key",
                                "id_kota": "kota_key",
                                "tanggal_mulai_proyek": "waktu_mulai_proyek_key",
                                "tanggal_selesai_proyek": "waktu_selesai_proyek_key",
                                "dana_anggaran": "dana_proyek"
                            })

    cursor = data_mart.cursor()
    cursor.execute("TRUNCATE fact_proyek RESTART IDENTITY CASCADE")
    petl.todb(fact_proyek, cursor, "fact_proyek")