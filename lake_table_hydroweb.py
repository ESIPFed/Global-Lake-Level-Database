# %% Section: MetaInfo
__author__ = ['John Franey', 'Jake Gearon']
__credits__ = ['John Franey', 'Jake Gearon', 'Earth Science Information Partners (ESIP)']
__version__ = '1.0.0'
__maintainer__ = 'John Franey'
__email__ = 'franeyjohn96@gmail.com'
__status__ = 'Development'

def update_hydroweb_lake_levels(data_table):
    """
    Update Lake Water Levels from the [HydroWeb Database](http://hydroweb.theia-land.fr/)

    :return: None
    """
    import pandas as pd
    from sqlalchemy import create_engine
    from io import BytesIO
    from zipfile import ZipFile
    import urllib.request
    from utiils import printProgressBar
    import config

    # %% Section: MetaInfo
    __author__ = 'John Franey'
    __credits__ = ['John Franey', 'Jake Gearon']

    __version__ = '1.0.0'
    __maintainer__ = 'John Franey'
    __email__ = 'franeyjohn96@gmail.com'
    __status__ = 'Development'
    # %%
    username = config.username
    password = config.password
    sql_engine = create_engine('mysql+pymysql://' + username + ':' + password +
                               '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()
    hydroweb_sql = u"SELECT `id_No`," \
                   u"`lake_name` " \
                   u" FROM reference_ID WHERE `source` = 'hydroweb'"

    # Read in Grealm unique lake ID and observation dates from reference Table
    # Clean up the grealm_lakes_info dataframe
    hydroweb_lakes_info = pd.read_sql(hydroweb_sql, con=sql_engine)
    # %% Section: Create df of all available hydroweb data
    ls_df = []
    url = "http://hydroweb.theia-land.fr/hydroweb/authdownload?products=lakes&user=jake.gearon@gmail.com&pwd=vHs98NdXe9BxWNyok*Pp&format=txt"
    target_url = urllib.request.urlopen(url)


    with ZipFile(BytesIO(target_url.read())) as my_zip_file:
        printProgressBar(0, len(my_zip_file.namelist()), prefix='HydroWeb Lake Data Update:',
                         suffix='Complete',
                         length=50)
        for count, contained_file in enumerate(my_zip_file.namelist(), 1):
            name = contained_file[11:-4]
            df = pd.read_csv(my_zip_file.open(contained_file),
                             sep=';',
                             comment='#',
                             skiprows=1,
                             index_col=False,
                             parse_dates=[1],
                             infer_datetime_format=True,
                             names=['Decimal Year', 'date', 'Time | hh:mm',
                                    'lake_level', 'Standard deviation from height (m)',
                                    'Area (km2)', 'Volume with respect to volume of first date (km3)', 'Flag']
                             )
            df['lake_name'] = name.capitalize()
            # unique_id = hydroweb_lakes_info.loc[hydroweb_lakes_info['lake_name'] == name, 'id_No']
            ls_df.append(df)
            printProgressBar(count + 1, len(my_zip_file.namelist()), prefix='HydroWeb Lake Data Update:',
                             suffix='Complete',
                             length=50)
    raw_lake_level_df = pd.concat(ls_df, ignore_index=True, copy=False)

    # %% Section: filter source df
    filtered_lake_levels = pd.DataFrame()
    filtered_lake_levels['date'] = raw_lake_level_df[raw_lake_level_df.columns[1]].dt.strftime('%Y-%m-%d')
    filtered_lake_levels['water_level'] = raw_lake_level_df[raw_lake_level_df.columns[3]]
    filtered_lake_levels['lake_name'] = raw_lake_level_df[raw_lake_level_df.columns[-1]]
    # %% Section
    id_labeled_df = pd.merge(hydroweb_lakes_info, filtered_lake_levels, on=['lake_name'])
    existing_database_df = data_table

    existing_database_df['date'] = pd.to_datetime(existing_database_df['date'])
    raw_lake_level_df['date'] = pd.to_datetime(raw_lake_level_df['date'])
    # sql_ready_df = pd.merge(id_labeled_df, existing_database_df,
    #                             indicator=True,
    #                             how='outer',
    #                             on=['id_No', 'date'],
    #                             ).query('_merge=="left_only"').drop('_merge', axis=1)

    sql_ready_df = pd.concat([existing_database_df, raw_lake_level_df]).drop_duplicates(subset=['id_No', 'date'],
                                                                                        keep=False).reset_index(
        drop=True)
    sql_ready_df = sql_ready_df.drop(['lake_name_y', 'water_level_y'], axis=1)
    sql_ready_df = sql_ready_df.rename(columns={'lake_name_x': 'lake_name', 'water_level_x': 'water_level'})
    sql_ready_df = sql_ready_df.drop_duplicates(subset=['id_No', 'date'], keep=False)

    sql_ready_df.to_sql('lake_water_level',
                        con=sql_engine,
                        index=False,
                        if_exists='append',
                        chunksize=2000
                        )
    print("HydroWeb Lake Levels Updated")