# %% Section: MetaInfo
__author__ = ['John Franey', 'Jake Gearon']
__credits__ = ['John Franey', 'Jake Gearon', 'Earth Science Information Partners (ESIP)']
__version__ = '1.0.0'
__maintainer__ = 'John Franey'
__email__ = 'franeyjohn96@gmail.com'
__status__ = 'Development'
def update_grealm_lake_levels(data_table):
    """
    Update Lake Water Levels from the [USDA-GREALM Database](https://ipad.fas.usda.gov/cropexplorer/global_reservoir/)
    :return: None
    """
    import pandas as pd
    from sqlalchemy import create_engine
    import config
    from utiils import get_lake_table

    username = config.username
    password = config.password
    # Create database connection engines and cursor
    sql_engine = create_engine('mysql+pymysql://' + username + ':' + password +
                               '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()

    grealm_sql = u"SELECT `id_No`," \
                 u"`lake_name`," \
                 u"metadata->'$.grealm_database_ID' AS `grealm_ID`," \
                 u"metadata->'$.\"Satellite Observation Period\"' AS `observation_date`" \
                 u" FROM reference_ID WHERE `source` = 'grealm'"

    # Read in Grealm unique lake ID and observation dates from reference Table
    # Clean up the grealm_lakes_info dataframe

    grealm_lakes_info = pd.read_sql(grealm_sql, con=sql_engine)
    grealm_lakes_info['grealm_ID'] = grealm_lakes_info['grealm_ID'].str.strip('"')


    ls_df = []
    missing_data = []
    from utiils import printProgressBar
    printProgressBar(0, len(grealm_lakes_info['grealm_ID']), prefix='GREALM-USDA Lake Data Update:', suffix='Complete',
                     length=50)
    for count, (grealm_id, u_id, name) in enumerate(zip(grealm_lakes_info['grealm_ID'],
                                     grealm_lakes_info['id_No'],
                                     grealm_lakes_info['lake_name']), 1):
        # TODO: attempt to get both 10 and 27 day data, merge into 1 df, then add to sql
        try:
            target_url = 'https://ipad.fas.usda.gov/lakes/images/lake{}.10d.2.txt'.format(grealm_id.zfill(4))
            source_df = pd.read_csv(target_url, skiprows=49, sep='\s+', header=None, parse_dates={'date': [2, 3, 4]},
                                    na_values=[99.99900, 999.99000, 9999.99000], infer_datetime_format=True,
                                    error_bad_lines=False, skip_blank_lines=True)
        except Exception as e:
            #print('*******************************************************')
            #print(e)
            #print('10 day summary does not exist for {}: {}\nRedirecting to 27 day average'.format(grealm_id, name))

            try:
                target_url = 'https://ipad.fas.usda.gov/lakes/images/lake{}.27a.2.txt'.format(grealm_id.zfill(4))
                source_df = pd.read_csv(target_url, skiprows=49, sep='\s+', header=None, parse_dates={'date': [2, 3, 4]},
                                        na_values=[99.99900, 999.99000, 9999.99000], infer_datetime_format=True,
                                        error_bad_lines=False, skip_blank_lines=True)
            except Exception as e:
                #print(e)
                #print('No Data found for {}: {}'.format(grealm_id, name))
                missing_data.append((grealm_id, name))
                continue

        source_df = source_df[['date', 14]]
        source_df = source_df.dropna()
        source_df['date'] = pd.to_datetime(source_df['date'], format='%Y%m%d %H %M')
        source_df['date'] = source_df['date'].dt.strftime('%Y-%m-%d')
        source_df.insert(0, 'id_No', u_id)
        source_df.insert(2, 'lake_name', name)
        source_df = source_df.rename(columns={14: 'water_level'})
        ls_df.append(source_df)
        printProgressBar(count + 1, len(grealm_lakes_info['grealm_ID']), prefix='GREALM-USDA Lake Data Update:',
                         suffix='Complete',
                         length=50)
    raw_lake_level_df = pd.concat(ls_df, ignore_index=True, copy=False)
    print('There were {} lake(s) where no GREALM-USDA information could be located'.format(len(missing_data)))
    existing_database_df = get_lake_table()

    existing_database_df['date'] = pd.to_datetime(existing_database_df['date'])
    raw_lake_level_df['date'] = pd.to_datetime(raw_lake_level_df['date'])

    # sql_ready_df = raw_lake_level_df.merge(existing_database_df, how='left', indicator=True).query('_merge == "left_only"').drop(['_merge'], axis=1)
    sql_ready_df = pd.concat([existing_database_df, raw_lake_level_df]).drop_duplicates(subset=['id_No', 'date'], keep=False).reset_index(drop=True)

    sql_ready_df.to_sql('lake_water_level',
                        con=sql_engine,
                        index=False,
                        if_exists='append',
                        chunksize=2000
                        )
    print("GREALM-USDA Lake Levels Updated")
    sql_engine.close()
