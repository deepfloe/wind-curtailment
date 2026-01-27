from ElexonDataPortal import api
from dotenv import load_dotenv
import os

load_dotenv()
key = os.getenv("ELEXXON_KEY")
client = api.Client(key)
start_date = '2023-01-01'
end_date = '2023-01-01 00:30'

#https://osuked.github.io/ElexonDataPortal/
df_BOD = client.get_BOD(start_date, end_date)

#df_fpn = client.get_PHYBMDATA(start_date, end_date)

print(df_BOD.head(3))


#https://api.bmreports.com/BMRS/PHYBMDATA/v1?APIKey=lhy9ye804l3qs7x&SettlementDate=2020-01-01&SettlementPeriod=1&BMUnitId=%2A&BMUnitType=%2A&LeadPartyName=%2A&NGCBMUnit=%2A&Name=%2A&ServiceType=xml