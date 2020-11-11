def update_grealm_lake_levels():
    import pandas as pd
    from sqlalchemy import create_engine
    import pymysql


    # Create database connection engines and cursor
    sql_engine = create_engine('mysql+pymysql://***REMOVED***:***REMOVED***'
                               '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()
    connection = pymysql.connect(host='lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com',
                                 user='***REMOVED***',
                                 password='***REMOVED***',
                                 db='laketest',
                                 connect_timeout=100000,
                                 )
    cursor = connection.cursor()

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
    for grealm_id, u_id, name in zip(grealm_lakes_info['grealm_ID'],
                                     grealm_lakes_info['id_No'],
                                     grealm_lakes_info['lake_name']):

        try:
            target_url = 'https://ipad.fas.usda.gov/lakes/images/lake{}.10d.2.txt'.format(grealm_id.zfill(4))
            source_df = pd.read_csv(target_url, skiprows=49, sep='\s+', header=None, parse_dates={'date': [2, 3, 4]},
                                    na_values=[99.99900, 999.99000, 9999.99000], infer_datetime_format=True,
                                    error_bad_lines=False, skip_blank_lines=True)
        except Exception as e:
            print('*******************************************************')
            print(e)
            print('10 day summary does not exist for {}: {}\nRedirecting to 27 day average'.format(grealm_id, name))

            try:
                target_url = 'https://ipad.fas.usda.gov/lakes/images/lake{}.27a.2.txt'.format(grealm_id.zfill(4))
                source_df = pd.read_csv(target_url, skiprows=49, sep='\s+', header=None, parse_dates={'date': [2, 3, 4]},
                                        na_values=[99.99900, 999.99000, 9999.99000], infer_datetime_format=True,
                                        error_bad_lines=False, skip_blank_lines=True)
            except Exception as e:
                print(e)
                print('No Data found for {}: {}'.format(grealm_id, name))
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
    raw_lake_level_df = pd.concat(ls_df, ignore_index=True, copy=False)
    print('There were {} lake(s) where no G-Realm information could be located'.format(len(missing_data)))

    existing_database_df = pd.read_sql('lake_water_level', con=sql_engine)
    existing_database_df['date'] = existing_database_df['date'].dt.strftime('%Y-%m-%d')

    sql_ready_df = pd.merge(raw_lake_level_df, existing_database_df,
                            indicator=True,
                            how='outer',
                            on=['id_No', 'date'],
                            ).query('_merge=="left_only"').drop('_merge', axis=1)

    sql_ready_df = sql_ready_df.drop(['lake_name_y', 'water_level_y'], axis=1)
    sql_ready_df = sql_ready_df.rename(columns={'lake_name_x': 'lake_name', 'water_level_x': 'water_level'})
    sql_ready_df = sql_ready_df.drop_duplicates(subset=['id_No', 'date'])

    sql_ready_df.to_sql('lake_water_level',
                        con=sql_engine,
                        index=False,
                        if_exists='append',
                        chunksize=2000
                        )

    connection.close()
