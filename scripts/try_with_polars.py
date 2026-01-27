import requests
import polars as pl

def concact_from_and_to(lf, base_columns):
    from_columns = ['TimeFrom', 'LevelFrom']
    to_columns = ['TimeTo', 'LevelTo']
    lf = lf.select(base_columns + from_columns+to_columns)
    lf_from = lf.select(base_columns+from_columns).rename({'TimeFrom':'Time', 'LevelFrom':'Level'})
    lf_to = lf.select(base_columns+to_columns).rename({'TimeTo':'Time', 'LevelTo':'Level'})

    return pl.concat([lf_from, lf_to], how='vertical')

pns = []
schema = {
    'BmUnit': pl.String, 
    'LevelFrom': pl.Float64, 
    'LevelTo': pl.Float64, 
    'TimeFrom': pl.Datetime,
    'TimeTo': pl.Datetime
}
for s in range(1, 51):

    r = requests.get(
        "https://data.elexon.co.uk/bmrs/api/v1/balancing/physical/all",
        params={"dataset": "PN", 
                "settlementDate": "2022-10-22", 
                "settlementPeriod": s, 
                "format": "csv",},
    )
    to_append = pl.scan_csv(r.content).select(
        [pl.col(s).cast(t) for s,t in schema.items()]
    )
    pns.append(to_append)

pn = pl.concat(pns, how='vertical')


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


# split from/to into long format. This should happed before the datetime stuff
# Only have the columns "t" and "Level"
# we want to resolve the issue with multiple bids, what they did here
#resolve_applied_bid_offer_level: pick the dataframe with the latest acceptance number
# There's an issue that the bids won't be at the same time
# they're doing the following:
# 
# They are relying on B< units, not national grid unit
# converting some time units


base_columns = ['AcceptanceNumber', 'BmUnit']
from_columns = ['TimeFrom', 'LevelFrom']
to_columns = ['TimeTo', 'LevelTo']

boalf = boalf.select(
    base_columns + from_columns+to_columns
    ).pipe(
        concact_from_and_to, 
        base_columns=['AcceptanceNumber', 'BmUnit']
    ).with_columns(
        pl.col('Time').cast(pl.Datetime)
    ).sort(
        by='Time'
    ).collect().upsample(
        time_column='Time', 
        group_by=['AcceptanceNumber','BmUnit'], 
        every='1m', 
        maintain_order=True
    ).select(
        pl.all().fill_null(strategy="forward")
    ).group_by('Time').last()


pn = pn.pipe(
        concact_from_and_to, 
        base_columns=['BmUnit']
    ).with_columns(
        pl.col('Time').cast(pl.Datetime)
    ).collect().sort(
        by='Time'
    ).upsample(
        time_column='Time',
        group_by=['BmUnit'],
        every='1m',
        maintain_order=True,
    ).interpolate()
#pn = pn.filter(pl.col('BmUnit')=='T_WBURB-3')

merged = pn.join(boalf, on=['BmUnit','Time'], how='left', suffix='_BOALF').rename({'Level': 'Level_PN'}).with_columns(
        pl.col('Level_BOALF').fill_null(pl.col('Level_PN'))
    ).with_columns(
        delta = (pl.col('Level_PN')-pl.col('Level_BOALF'))
    )

merged.write_parquet("2022-10-22.parquet")