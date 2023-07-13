import petl
from load_dim import load_dim

def dim_donor(data_mart, operasional):
    print("===DIM DONOR===")

    donor = petl.fromdb(operasional, "SELECT * FROM donor")
    dim_donor = petl.fromdb(data_mart, "SELECT * FROM dim_donor")
    dim_negara = petl.fromdb(data_mart, "SELECT * FROM dim_negara")
    lkp_negara = petl.dictlookupone(dim_negara, "id_negara")

    donor = petl.convert(
        donor, {"id_negara": lambda id_negara: lkp_negara[id_negara]["negara_key"]})
    donor = petl.rename(donor, {"id_negara": "negara_key"})

    cursor = data_mart.cursor()
    load_dim("dim_donor", dim_donor, donor, "donor_key", "id_donor", cursor)
