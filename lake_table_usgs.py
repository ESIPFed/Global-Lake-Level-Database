def update_usgs_lake_levels():
    from usgs_datagrab import get_usgs_sites
    from datetime import datetime
    import requests
    from requests.exceptions import HTTPError
    from time import process_time
    import pandas as pd
    from sqlalchemy import create_engine
    import pymysql
    from utiils import printProgressBar
# %% Section: MetaInfo
    __author__ = ['John Franey', 'Jake Gearon']
    __credits__ = ['John Franey', 'Jake Gearon']

    __version__ = '1.0.0'
    __maintainer__ = 'John Franey'
    __email__ = 'franeyjohn96@gmail.com'
    __status__ = 'Development'



    pr_start = process_time()

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
    # todo for updating, only grab active sites!
    # todo use modified since for updating!
    # Read in usgs unique lake ID and observation dates from reference Table
    usgs_lakes_info = pd.read_sql(usgs_sql, con=sql_engine)
    # %%
    sites = get_usgs_sites()
    df_ls = []
    missing_sites = []
    printProgressBar(0, len(sites), prefix='Progress:', suffix='Complete', length=50)
    for count, site in enumerate(sites, 1):
            target_url = 'http://waterservices.usgs.gov/nwis/dv/?sites={}&siteType=LK&startDT={}&endDT={}' \
                         '&statCd=00003,00011,00001,32400,30800,30600&format=json&variable=00062,00065,' \
                         '30211,62600,62614,62615,62616,62617,62618,72020,' \
                         '72292,72293,72333,99020,72178,72199,99065,30207,' \
                         '72214,72264,72275,72335,72336'.format(site, begin_date,end_date).replace("%2C", ",")
            #print("site number: {}".format(site))
            t1_start = process_time()
            try:
                response = requests.get(target_url)
                response.raise_for_status()
                # access JSOn content
                jsonResponse = response.json()
                site_name = jsonResponse["value"]["timeSeries"][0]['sourceInfo']['siteName']
                df = pd.DataFrame.from_dict(jsonResponse["value"]['timeSeries'][0]["values"][0]['value'],
                                            orient="columns").drop('qualifiers', axis=1)
                df["lake_name"] = site_name
                df_ls.append(df)

            except HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')
            except IndexError as e:
                print(e)
                print('site data for {} not found, check parameters!'.format(site))
                missing_sites.append(site)
                print('*************************************')
            except Exception as err:
                print(f'Other error occurred: {err}')

            #print("Lake {}/{}".format(count, len(sites)))
            printProgressBar(0, len(sites), prefix='Progress:', suffix='Complete', length=50)

            t1_stop = process_time()
            print("Elapsed time during the whole program in seconds:",
                  t1_stop - t1_start)


    pr_stop = process_time()
    print("Elapsed time during the entire process in seconds:",
          pr_stop - pr_start)
    # %%
    usgs_source_df = pd.concat(df_ls, ignore_index=True, copy=False)
    usgs_source_df["date"] = pd.to_datetime(usgs_source_df["dateTime"], format='%Y-%m-%d')
    id_labeled_df = pd.merge(usgs_lakes_info, usgs_source_df, on=['lake_name'])
    # usgs_source_df['date'] = usgs_source_df['dateTime'].dt.strftime('%Y-%m-%d')
    id_labeled_df['date'] = id_labeled_df['date'].dt.strftime('%Y-%m-%d')
    id_labeled_df = id_labeled_df.drop(['dateTime'], axis=1)
    id_labeled_df = id_labeled_df.rename(columns={'value': 'water_level'})
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