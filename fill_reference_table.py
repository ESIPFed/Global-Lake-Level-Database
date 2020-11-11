def replace_reference_id_table():
    """gathers all lake names from data sources, assigns a unique ID number
    This should only be used on initial DB creation
    else use the update_reference_id_table"""
    import pandas as pd
    from sqlalchemy import create_engine
    import pymysql
    from lake_table_usgs import get_usgs_sites
    from lake_table_usgs import update_usgs_meta

    confirmation = input('Are you sure you want to replace the entire database?\nType "yes" to continue: ')
    if confirmation is not 'yes':
        print('Confirmation not valid. No action taken')
    else:
        # Create database connection engines and cursor
        sql_engine = create_engine('mysql+pymysql://***REMOVED***:***REMOVED***'
                                   '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()

        # Get lake names from hydroweb, drop metadata, add source info
        print('--------Gathering Hydroweb lake information--------')
        hydroweb_url = 'http://hydroweb.theia-land.fr/hydroweb/authdownload?list=lakes&format=txt'
        hydroweb_df = pd.read_csv(hydroweb_url)
        hydroweb_df = hydroweb_df.rename(columns={'lake': 'lake_name'})
        hydroweb_df = hydroweb_df.filter(['lake_name'])
        hydroweb_df.insert(0, 'source', 'hydroweb')

        # Get lake names from usgs, drop metadata, add source info
        print('--------Gathering USGS lake information--------')
        usgs_df = update_usgs_meta()
        usgs_df = usgs_df.rename(columns={'name': 'lake_name'})
        usgs_df = usgs_df.filter(['lake_name'])
        usgs_df.insert(0, 'source', 'usgs')

        # Get lake names from grealm, drop metadata, add source info
        print('--------Gathering G-Realm lake information--------')
        grealm_url = 'https://ipad.fas.usda.gov/lakes/images/LakesReservoirsCSV.txt'
        grealm_df = pd.read_csv(grealm_url, skiprows=3, sep="\t", header=0, parse_dates=[-1],
                                infer_datetime_format=True, error_bad_lines=False, skip_blank_lines=True)
        grealm_df = grealm_df[~grealm_df['Lake ID'].str.contains("Total")]
        grealm_df = grealm_df.rename(columns={'Name': 'lake_name'})
        grealm_df = grealm_df.filter(['lake_name'])
        grealm_df.insert(0, 'source', 'grealm')

        lake_reference_df = pd.concat([hydroweb_df, usgs_df, grealm_df], ignore_index=True)

        print('--------Overwriting database--------')
        lake_reference_df.to_sql('reference_ID', con=sql_engine, if_exists='replace', index_label='id_No')


def update_reference_id_table():
    import pandas as pd
    from sqlalchemy import create_engine
    import pymysql
    from lake_table_usgs import get_usgs_sites
    from lake_table_usgs import update_usgs_meta

    # Create database connection engines and cursor
    sql_engine = create_engine('mysql+pymysql://***REMOVED***:***REMOVED***'
                               '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()
    connection = pymysql.connect(host='lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com',
                                 user='***REMOVED***',
                                 password='***REMOVED***',
                                 db='laketest')
    cursor = connection.cursor()

    reference_table = pd.read_sql('reference_ID', con=sql_engine, columns=['id_No', 'lake_name', 'source'])
    print('Reference Table Read')

    grealm_url = 'https://ipad.fas.usda.gov/lakes/images/LakesReservoirsCSV.txt'
    grealm_source_df = pd.read_csv(grealm_url, skiprows=3, sep="\t", header=0, parse_dates=[-1],
                                   infer_datetime_format=True, error_bad_lines=False, skip_blank_lines=True)
    grealm_source_df = grealm_source_df[~grealm_source_df['Lake ID'].str.contains("Total")]
    grealm_source_df = grealm_source_df.rename(columns={'Name': 'lake_name'})
    grealm_source_df = grealm_source_df.filter(['lake_name'])


    # Merge reference and grealm tables while keeping unique lake ID number from db, convert to json dict
    grealm_existing_lakes_table = reference_table.loc[reference_table['source'] == 'grealm']
    grealm_ready_df = grealm_source_df[~grealm_source_df.lake_name.isin(grealm_existing_lakes_table['lake_name'])]
    grealm_ready_df.insert(0, 'source', 'grealm')
    print("GREALM-USDA Metadata Updated")

    # Grab hydroweb source data
    hydroweb_url = 'http://hydroweb.theia-land.fr/hydroweb/authdownload?list=lakes&format=txt'
    hydroweb_source_df = pd.read_csv(hydroweb_url)
    hydroweb_source_df = hydroweb_source_df.rename(columns={'lake': 'lake_name'})
    hydroweb_source_df = hydroweb_source_df.filter(['lake_name'])

    hydroweb_existing_lake_df = reference_table.loc[reference_table['source'] == 'hydroweb']
    hydroweb_ready_df = hydroweb_source_df[~hydroweb_source_df.lake_name.isin(hydroweb_existing_lake_df['lake_name'])]
    hydroweb_ready_df.insert(0, 'source', 'hydroweb')
    print("HydroWeb Metadata Updated")

    # Grab usgs source data
    usgs_source_df_raw = update_usgs_meta()
    usgs_source_df = usgs_source_df_raw.rename(columns={'station_nm': 'lake_name'})
    usgs_source_df = usgs_source_df.filter(['lake_name'])

    usgs_existing_lake_df = reference_table.loc[reference_table['source'] == 'usgs']
    usgs_ready_df = usgs_source_df[~usgs_source_df.lake_name.isin(usgs_existing_lake_df['lake_name'])]
    usgs_ready_df.insert(0, 'source', 'usgs')
    print("USGS-NWIS Metadata Updated")

    sql_ready_df = pd.concat([grealm_ready_df, hydroweb_ready_df, usgs_ready_df], ignore_index=True)

    sql_command = "INSERT IGNORE INTO reference_ID(lake_name, `source`) VALUES (%s, %s);"
    for name, source in zip(sql_ready_df['lake_name'], sql_ready_df['source']):
        cursor.execute(sql_command, (name, source))
    connection.commit()
    connection.close()
    return usgs_source_df_raw
