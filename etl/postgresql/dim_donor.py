import petl

def dim_donor(data_mart, operasional):
    print("===DIM DONOR===")

    donor = petl.fromdb(operasional, "SELECT * FROM donor")
    dim_negara = petl.fromdb(data_mart, "SELECT * FROM dim_negara")
    lkp_negara = petl.dictlookupone(dim_negara, "id_negara")

    donor = petl.convert(donor, {"id_negara": lambda id_negara : lkp_negara[id_negara]["negara_key"]})
    donor = petl.rename(donor, {"id_negara": "negara_key"})

    cursor = data_mart.cursor()
    cursor.execute("TRUNCATE dim_donor RESTART IDENTITY CASCADE")
    petl.todb(donor, cursor, "dim_donor")