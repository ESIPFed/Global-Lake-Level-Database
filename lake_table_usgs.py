import pandas as pd
from sqlalchemy import create_engine
import pymysql
from usgs_datagrab import get_usgs_sites
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from lxml import etree

sql_engine = create_engine('mysql+pymysql://***REMOVED***:***REMOVED***'
                               '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()
connection = pymysql.connect(host='lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com',
                             user='***REMOVED***',
                             password='***REMOVED***',
                             db='laketest',
                             connect_timeout=100000,
                             )
cursor = connection.cursor()

usgs_sql = u"SELECT `id_No`," \
           u"`lake_name` " \
           u" FROM reference_ID WHERE `source` = 'usgs'"

begin_date = '1838-01-01'
now = datetime.now()
end_date = now.strftime('%Y-%m-%d')

# Read in usgs unique lake ID and observation dates from reference Table
# Clean up the grealm_lakes_info dataframe
usgs_lakes_info = pd.read_sql(usgs_sql, con=sql_engine)
# %%
sites = get_usgs_sites()
df_ls = []
missing_sites = []
for count, site in enumerate(sites, 1):
    df = pd.DataFrame(columns=['lake_name', 'date', 'water_level'])
    if (count*100) / len(sites) == 25:
        print('------------------------------')
        print('25% complete')
        print('------------------------------')
    if (count*100) / len(sites) == 50:
        print('------------------------------')
        print('50% complete')
        print('------------------------------')
    if (count*100) / len(sites) == 75:
        print('------------------------------')
        print('75% complete')
        print('------------------------------')
    try:
        target_url = 'http://waterservices.usgs.gov/nwis/dv/?sites={}&siteType=LK&startDT={}&endDT={}' \
                     '&format=waterml&variable=00062,' \
                     '30211,62600,62614,62615,62616,62617,62618,72020,' \
                     '72292,72293,72333,99020,72178,72199'.format(site, begin_date, end_date).replace("%2C", ",")
        print("site number: {}".format(site))
        req = requests.get(target_url)
        soup = BeautifulSoup(req.content, "lxml-xml")
        site_name = soup.find("siteName").get_text()

        for elem in soup.find_all(attrs={"qualifiers": ['A', 'R', 'P', 'A R']}):
            value = (elem.get_text())
            datetime = elem.attrs['dateTime']
            df = df.append({"lake_name": site_name, "date": datetime, "water_level": value}, ignore_index=True)
    except AttributeError as e:
        print(e)
        print('site data for {} not found'.format(site))
        missing_sites.append(site)
        print('*************************************')
    df_ls.append(df)
# %% Scratch
# site = sites[0]
# target_url = 'http://waterservices.usgs.gov/nwis/dv/?sites={}&siteType=LK&startDT={}&endDT={}' \
#                  '&format=waterml&variable=00062,' \
#                  '30211,62600,62614,62615,62616,62617,62618,72020,' \
#                  '72292,72293,72333,99020,72178,72199'.format(site, begin_date, end_date).replace("%2C", ",")
# print("site number: {}".format(site))
# req = requests.get(target_url)
# soup = BeautifulSoup(req.content, "lxml-xml")
# site_name = soup.find("siteName").get_text()
# # for elem in soup.find_all(attrs={'qualifiers': ["P", "A", "R", "A R"]}):
# for elem in soup.find_all("ns1:value"):
#     value = (elem.get_text())
#     datetime = elem.attrs['dateTime']
#     df = df.append({"lake_name": site_name, "date": datetime, "water_level": value}, ignore_index=True)
#     value = (elem.get_text())
#     datetime = elem.attrs['dateTime']
#     df = df.append({"lake_name": site_name, "date": datetime, "water_level": value}, ignore_index=True)


# %%
usgs_source_df = pd.concat(df_ls, ignore_index=True, copy=False)
usgs_source_df['date'] = usgs_source_df['date'].dt.strftime('%Y-%m-%d')
id_labeled_df = pd.merge(usgs_lakes_info, usgs_source_df, on=['lake_name'])
id_labeled_df['date'] = id_labeled_df['date'].dt.strftime('%Y-%m-%d')
existing_database_df = pd.read_sql('lake_water_level', con=sql_engine)
existing_database_df['date'] = existing_database_df['date'].dt.strftime('%Y-%m-%d')

sql_ready_df = pd.merge(id_labeled_df, existing_database_df,
                        indicator=True,
                        how='outer',
                        on=['id_No', 'date'],
                        ).query('_merge=="left_only"').drop('_merge', axis=1)

sql_ready_df = sql_ready_df.drop(['lake_name_y', 'water_level_y'], axis=1)
sql_ready_df = sql_ready_df.rename(columns={'lake_name_x': 'lake_name', 'water_level_x': 'water_level'})
sql_ready_df = sql_ready_df.drop_duplicates(subset=['id_No', 'date'])
# %%
sql_ready_df.to_sql('lake_water_level',
                    con=sql_engine,
                    index=False,
                    if_exists='append',
                    chunksize=2000
                    )
if not missing_sites:
    print('All sites processed')
else:
    print('Missing {} out of {} sites.'.format(len(missing_sites), len(sites)))
    print('Missing Site IDs:')
    for site in missing_sites:
        print(site)

connection.close()