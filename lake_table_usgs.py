# %% Section: MetaInfo
__author__ = ['John Franey', 'Jake Gearon']
__credits__ = ['John Franey', 'Jake Gearon']

__version__ = '1.0.0'
__maintainer__ = 'John Franey'
__email__ = 'franeyjohn96@gmail.com'
__status__ = 'Development'
#%% packages
# todo: add source column to lake_water_level so when we pd.read_sql we can filter more quickly.
def update_usgs_lake_levels(data_table):
    """
    writes in usgs lake level data to existing database, appending new data dynamically
    :return: None
    """
    from datetime import datetime
    import requests
    from requests.exceptions import HTTPError
    import pandas as pd
    from sqlalchemy import create_engine
    import pymysql
    from utiils import printProgressBar
    import config

    username = config.username
    password = config.password

    sql_engine = create_engine('mysql+pymysql://' + username + ':' + password +
                               '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()
    connection = pymysql.connect(host='lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com',
                                 user=username,
                                 password=password,
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
    printProgressBar(0, len(sites), prefix='USGS Lake Data Update:', suffix='Complete', length=50)
    for count, site in enumerate(sites, 1):
        target_url = 'http://waterservices.usgs.gov/nwis/dv/?sites={}&siteType=LK&startDT={}&endDT={}' \
                     '&statCd=00003,00011,00001,32400,30800,30600&format=json&variable=00062,00065,' \
                     '30211,62600,62614,62615,62616,62617,62618,72020,' \
                     '72292,72293,72333,99020,72178,72199,99065,30207,' \
                     '72214,72264,72275,72335,72336'.format(site, begin_date,end_date).replace("%2C", ",")
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
        except Exception as err:
            print(f'Other error occurred: {err}')

        printProgressBar(count+1, len(sites), prefix='USGS Lake Data Update:', suffix='Complete', length=50)

    # %%
    usgs_source_df = pd.concat(df_ls, ignore_index=True, copy=False)
    usgs_source_df["date"] = pd.to_datetime(usgs_source_df["dateTime"], format='%Y-%m-%d')
    id_labeled_df = pd.merge(usgs_lakes_info, usgs_source_df, on=['lake_name'])
    id_labeled_df['date'] = id_labeled_df['date'].dt.strftime('%Y-%m-%d')
    id_labeled_df = id_labeled_df.drop(['dateTime'], axis=1)
    id_labeled_df = id_labeled_df.rename(columns={'value': 'water_level'})

    existing_database_df = data_table

    existing_database_df['date'] = pd.to_datetime(existing_database_df['date'])
    id_labeled_df['date'] = pd.to_datetime(id_labeled_df['date'])

    # sql_ready_df = pd.merge(id_labeled_df, existing_database_df,
    #                         indicator=True,
    #                         how='outer',
    #                         on=['id_No', 'date'],
    #                         ).query('_merge=="left_only"').drop('_merge', axis=1)
    sql_ready_df = pd.concat([existing_database_df, id_labeled_df]).drop_duplicates(subset=['id_No', 'date'],
                                                                                        keep=False).reset_index(
        drop=True)
    # sql_ready_df = sql_ready_df.drop(['lake_name_y', 'water_level_y'], axis=1)
    # sql_ready_df = sql_ready_df.rename(columns={'lake_name_x': 'lake_name', 'water_level_x': 'water_level'})
    # sql_ready_df = sql_ready_df.drop_duplicates(subset=['id_No', 'date'], keep=False)
    # %%
    sql_ready_df['date'] = sql_ready_df['date'].dt.strftime('%Y-%m-%d')
    sql_ready_df = sql_ready_df.filter(['id_No', 'lake_name', 'water_level', 'date'])
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
        # print('Missing Site IDs:')
        # for site in missing_sites:
        #     print(site)
    print("USGS-NWIS Lake Levels Updated")
    connection.close()
#%% usgs funcs

def get_usgs_sites():
    """
    this function retrives every USGS site that has data from a set of pre-defined elevation parameters:

    :return: list of sites with water level data
    """
    from datetime import datetime
    import pandas as pd
    begin_date = '1838-01-01'
    now = datetime.now()
    end_date = now.strftime('%Y-%m-%d')
    # TODO Document what each parameter means or have a link to USGS website explaining
    #todo create dictionary of parameter id and names
    lake_list = 'https://waterdata.usgs.gov/nwis/dv?referred_module=sw&' \
                'site_tp_cd=LK&index_pmcode_72020=1&' \
                'index_pmcode_99020=1&' \
                'index_pmcode_30211=1&' \
                'index_pmcode_00062=1&' \
                'index_pmcode_99065=1&' \
                'index_pmcode_30207=1&' \
                'index_pmcode_00065=1&' \
                'index_pmcode_62600=1&' \
                'index_pmcode_72292=1&' \
                'index_pmcode_72293=1&' \
                'index_pmcode_62615=1&' \
                'index_pmcode_62614=1&' \
                'index_pmcode_62616=1&' \
                'index_pmcode_62617=1&' \
                'index_pmcode_72214=1&' \
                'index_pmcode_72264=1&' \
                'index_pmcode_72275=1&' \
                'index_pmcode_72333=1&' \
                'index_pmcode_72335=1&' \
                'index_pmcode_72336=1&' \
                'index_pmcode_72199=1&' \
                'index_pmcode_72178=1&' \
                'group_key=NONE&format=sitefile_output&sitefile_output_format=rdb&state_cd=al&state_cd=ak&state_cd=aq' \
                '&state_cd=az&state_cd=ar&state_cd=bc&state_cd=ca&state_cd=eq&state_cd=co&state_cd=ct&state_cd=de' \
                '&state_cd=dc&state_cd=fl&state_cd=ga&state_cd=gu&state_cd=hi&state_cd=id&state_cd=il&state_cd=in' \
                '&state_cd=ia&state_cd=jq&' \
                'state_cd=ks&state_cd=ky&state_cd=la&state_cd=me&state_cd=md&state_cd=ma&state_cd=mi&state_cd=mq' \
                '&state_cd=mn&state_cd=ms&state_cd=mo&state_cd=mt&state_cd=ne&' \
                'state_cd=nv&state_cd=nh&state_cd=nj&state_cd=nm&state_cd=ny&state_cd=nc&state_cd=nd&state_cd=mp' \
                '&state_cd=oh&state_cd=ok&' \
                'state_cd=or&state_cd=pa&state_cd=pr&state_cd=ri&state_cd=yq&state_cd=sc&state_cd=sd&state_cd=sq' \
                '&state_cd=tn&state_cd=tx' \
                '&state_cd=tq&state_cd=bq&state_cd=iq&state_cd=ut&state_cd=vt&state_cd=vi&state_cd=va&state_cd=wq' \
                '&state_cd=wa&state_cd=wv&state_cd=wi&state_cd=wy&' \
                'site_tp_cd=LK&' \
                'column_name=site_no&column_name=station_nm&range_selection=date_range&' \
                'begin_date={}&end_date={}&date_format=YYYY-MM-DD&rdb_compression=file&' \
                'list_of_search_criteria=site_tp_cd%2Crealtime_parameter_selection'.format(begin_date, end_date)
    df = pd.read_csv(lake_list, sep="\t", comment="#", header=[0], low_memory=False).drop([0], axis=0)
    return df["site_no"].values


def update_usgs_lake_names():
    """
    Updates the meta table of valid sites and associated info
    :return: Pandas Dataframe of sites and associated information
    """
    from utiils import printProgressBar
    import pandas as pd
    # TODO Document what each parameter means or have a link to USGS website explaining
    sites = get_usgs_sites()
    printProgressBar(0, len(sites), prefix='USGS Metadata Update:', suffix='Complete', length=50)
    big_df = []
    for count, site in enumerate(sites, 1):
        lake_m_list = "https://waterservices.usgs.gov/nwis/site/?format=rdb&sites={}&" \
                      "siteOutput=expanded&siteType=LK&siteStatus=all&hasDataTypeCd=iv,dv".format(site)
        df2 = pd.read_csv(lake_m_list, sep="\t", comment="#", header=[0], low_memory=False).drop([0], axis=0)
        big_df.append(df2)
        printProgressBar(count + 1, len(sites), prefix='Retriving new USGS-NWIS Lakes:', suffix='Complete', length=50)
    return pd.concat(big_df)
