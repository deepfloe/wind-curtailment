from ElexonDataPortal import api

client = api.Client('lhy9ye804l3qs7x')

start_date = '2020-01-01'
end_date = '2020-01-01 1:30'

#https://osuked.github.io/ElexonDataPortal/
df_BOD = client.get_BOD(start_date, end_date)

df_fpn = client.get_PHYBMDATA(start_date, end_date)

df_BOD.head(3)