import requests as req

def get_lokasi(tingkat: str, id_lokasi: int = 0):
    url = f"https://sig.bps.go.id/rest-bridging/getwilayah?level={tingkat}&parent={id_lokasi}"
    print(url)
    res = req.get(url)

    return res.json()