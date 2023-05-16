from neo4j import GraphDatabase

uri = "bolt://localhost:7687/datamart"
with GraphDatabase.driver(uri, auth=("neo4j", "@Harris99")) as driver:
    session = driver.session()
    try:
        result = session.run(
            "MATCH (n:DimProvinsus) RETURN n.namaProvinsi as nama_provinsi")
        nama_provinsi = [record["nama_provinsi"] for record in result]
        print(nama_provinsi)
    finally:
        session.close()
