import petl
from datetime import date

def fact_proyek(data_mart, operasional):
    print("===FACT PROYEK===")

    cursor = data_mart.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.fact_proyek
    (
        proyek_key serial NOT NULL,
        isu_key integer,
        waktu_mulai_proyek_key integer NOT NULL,
        waktu_selesai_proyek_key integer,
        donor_key integer NOT NULL,
        nama_proyek character varying NOT NULL,
        dana_proyek bigint NOT NULL,
        satuan_anggaran character varying NOT NULL,
        id_proyek integer NOT NULL,
        negara_key integer,
        provinsi_key integer,
        kota_key integer,
        jumlah_kegiatan bigint NOT NULL DEFAULT 0,
        pengeluaran_proyek bigint NOT NULL DEFAULT 0,
        tanggal_load date,
        CONSTRAINT fact_proyek_pkey PRIMARY KEY (proyek_key),
        CONSTRAINT donor_key FOREIGN KEY (donor_key)
            REFERENCES public.dim_donor (donor_key) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        CONSTRAINT isu FOREIGN KEY (isu_key)
            REFERENCES public.dim_isu (isu_key) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        CONSTRAINT kota FOREIGN KEY (kota_key)
            REFERENCES public.dim_kabupaten_kota (kab_kota_key) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        CONSTRAINT negara FOREIGN KEY (negara_key)
            REFERENCES public.dim_negara (negara_key) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        CONSTRAINT provinsi FOREIGN KEY (provinsi_key)
            REFERENCES public.dim_provinsi (provinsi_key) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        CONSTRAINT waktu_mulai FOREIGN KEY (waktu_mulai_proyek_key)
            REFERENCES public.dim_waktu (waktu_key) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE RESTRICT,
        CONSTRAINT waktu_selesai FOREIGN KEY (waktu_selesai_proyek_key)
            REFERENCES public.dim_waktu (waktu_key) MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE RESTRICT
    )
    """)

    proyek = petl.fromdb(operasional, "SELECT * FROM proyek")

    # load seluruh dim yang dibutuhkan
    dim_isu = petl.fromdb(data_mart, "SELECT * FROM dim_isu")
    dim_donor = petl.fromdb(data_mart, "SELECT * FROM dim_donor")
    dim_waktu = petl.fromdb(data_mart, "SELECT * FROM dim_waktu")
    dim_negara = petl.fromdb(data_mart, "SELECT * FROM dim_negara")
    dim_provinsi = petl.fromdb(data_mart, "SELECT * FROM dim_provinsi")
    dim_kota = petl.fromdb(data_mart, "SELECT * FROM dim_kabupaten_kota")
    
    
    # membuat lookup table
    lkp_dim_isu = petl.dictlookupone(dim_isu, "id_isu")
    lkp_dim_waktu = petl.dictlookupone(dim_waktu, "tanggal")
    lkp_dim_donor = petl.dictlookupone(dim_donor, "id_donor")
    lkp_dim_negara = petl.dictlookupone(dim_negara, "id_negara")
    lkp_dim_provinsi = petl.dictlookupone(dim_provinsi, "id_provinsi")
    lkp_dim_kota = petl.dictlookupone(dim_kota, "id_kab_kota")

    # melakukan konversi tabel proyek menjadi fact_proyek dengan konversi id tabel yang terhubung menjadi tabel dimensi
    fact_proyek = petl.convert(proyek,
                               {
                                   "id_donor": lambda id_donor: lkp_dim_donor[id_donor]["donor_key"],
                                   "id_isu": lambda id_isu: lkp_dim_isu[id_isu]["isu_key"],
                                   "id_negara": lambda id_negara: lkp_dim_negara[id_negara]["negara_key"],
                                   "id_provinsi": lambda id_provinsi: lkp_dim_provinsi[id_provinsi]["provinsi_key"],
                                   "id_kota": lambda id_kota: lkp_dim_kota[id_kota]["kab_kota_key"],
                                   "tanggal_mulai_proyek": lambda tanggal_mulai_proyek: lkp_dim_waktu[tanggal_mulai_proyek]["waktu_key"],
                                   "tanggal_selesai_proyek": lambda tanggal_selesai_proyek: lkp_dim_waktu[tanggal_selesai_proyek]["waktu_key"]
                               })

    # load data fact_proyek untuk mendapatkan data last_load,
    # last_load digunakan untuk mengetahui apakah sebuah fact_proyek pernah di load.
    fact_proyek_data = petl.fromdb(
        data_mart, "select proyek_key, id_proyek, max(tanggal_load) as last_load from fact_proyek GROUP BY proyek_key")
    lkp_fact_proyek = petl.dictlookupone(fact_proyek_data, "id_proyek")
    
    # mendapatkan tanggal load saat ini
    load_date = date.today()

    # load data kegiatan
    kegiatan = petl.fromdb(operasional, "SELECT * FROM kegiatan")
    lkp_kegiatan = petl.dictlookup(kegiatan, "id_proyek")

    # menambahkan data jumlah_kegiatan berdasarkan data fact_proyek yang terakhir di load
    fact_proyek = petl.addfield(fact_proyek,
                                field="jumlah_kegiatan",
                                value=lambda row: len(list(filter(lambda kegiatan:  kegiatan["tanggal_pelaksanaan"] >
                                                                  (lkp_fact_proyek.get(kegiatan["id_proyek"], {}).get(
                                                                      "last_load") or date(1, 1, 1)),
                                                                  lkp_kegiatan[row["id_proyek"]]))))

    # menambahkan pengeluaran_proyek
    fact_proyek = petl.addfield(fact_proyek,
                                field="pengeluaran_proyek",
                                value=lambda row: sum(
                                    # map : mendapatkan pengeluaran dari tabel kegiatan
                                    map(lambda kegiatan: kegiatan["pengeluaran"], 
                                        # filter kegiatan yang tanggal_pelaksanaan nya setelah tanggal load terakhir (last_load)
                                        filter(lambda kegiatan:  kegiatan["tanggal_pelaksanaan"] >
                                                # mendapatkan last_load dengan lookup ke tabel fact_proyek dengan id_proyek
                                               (lkp_fact_proyek.get(kegiatan["id_proyek"], {})
                                                .get("last_load") or date(1, 1, 1)), lkp_kegiatan[row["id_proyek"]]))))

    # menambahkan tanggal_load
    fact_proyek = petl.addfield(fact_proyek,
                                field="tanggal_load",
                                value=load_date)

    # rename field menjadi key
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

    petl.appenddb(fact_proyek, cursor, "fact_proyek")
