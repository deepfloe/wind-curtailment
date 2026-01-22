import requests
import polars as pl

r = requests.get(
    "https://data.elexon.co.uk/bmrs/api/v1/balancing/physical/all",
    params={"dataset": "PN", 
            "settlementDate": "2021-10-22", 
            "settlementPeriod": 48, 
            "format": "csv",},
)
pn = pl.scan_csv(r.content)
r = requests.get(
    "https://data.elexon.co.uk/bmrs/api/v1/datasets/BOALF",
    params={
            "from":"2022-10-22T00:00Z",
            "to":"2022-10-23T00:00Z",
            "format":"csv"
            },
)
boalf = pl.scan_csv(r.content)
#.filter(pl.col('NationalGridBmUnit')=='NEE-NPE01')
