# %% Section: MetaInfo
__author__ = ['John Franey', 'Jake Gearon']
__credits__ = ['John Franey', 'Jake Gearon', 'Earth Science Information Partners (ESIP)']
__version__ = '1.0.0'
__maintainer__ = 'John Franey'
__email__ = 'franeyjohn96@gmail.com'
__status__ = 'Development'
def reference_table_metadata_json(usgs_tbl, lake_reference_df):
    """
    Update lake metadata within the Reference ID table
    :param usgs_table: dataframe
    :return:
    """
    import pandas as pd
    from sqlalchemy import create_engine
    from utiils import get_ref_table
    from utiils import printProgressBar
    import pymysql
    import json
    import config

    username = config.username
    password = config.password

    # Create database connection engines and cursor
    sql_engine = create_engine('mysql+pymysql://' + username + ':' + password +
                               '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()
    connection = pymysql.connect(host='lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com',
                                 user=username,
                                 password=password,
                                 db='laketest')
    cursor = connection.cursor()

    # Read in reference table for unique Lake ID and Lake name
    id_table = lake_reference_df

    # Read in grealm summary table and clean dataframe
    grealm_url = 'https://ipad.fas.usda.gov/lakes/images/LakesReservoirsCSV.txt'
    grealm_source_df = pd.read_csv(grealm_url, skiprows=3, sep="\t", header=0, parse_dates=[-1],
                                   infer_datetime_format=True, error_bad_lines=False, skip_blank_lines=True)
    grealm_source_df = grealm_source_df[~grealm_source_df['Lake ID'].str.contains("Total")]
    grealm_source_df = grealm_source_df.rename(columns={'Name': 'lake_name', 'Lake ID': 'grealm_database_ID'})

    # Rename lake name to <lake name>_<Resolution> if 2 or more versions of the lake exist
    grealm_source_df.loc[grealm_source_df.lake_name.duplicated(keep=False), 'lake_name'] =\
        grealm_source_df.loc[grealm_source_df.lake_name.duplicated(keep=False), 'lake_name'] + '_' +\
        grealm_source_df.loc[grealm_source_df.lake_name.duplicated(keep=False), 'Resolution'].astype(str)

    # Merge reference and grealm tables while keeping unique lake ID number from db, convert to json dict
    grealm_id_table = id_table[(id_table['source'] == 'grealm')]
    grealm_id_table = grealm_id_table.loc[grealm_id_table.index.difference(grealm_id_table.dropna().index)]
    grealm_id_table = grealm_id_table.drop(['metadata'], axis=1)

    df_grealm = grealm_id_table.merge(grealm_source_df, on='lake_name', how='inner')
    df_grealm = df_grealm.set_index('id_No')
    grealm_json = df_grealm.to_json(orient='index')
    try:
        grealm_dict = eval(grealm_json)
    except NameError:
        grealm_dict = {}

    print('grealm metadata prepped')

    # repeat process with hydroweb summary table, results in json dict with Unique lake ID
    hydroweb_url = 'http://hydroweb.theia-land.fr/hydroweb/authdownload?list=lakes&format=txt'
    hydroweb_df = pd.read_csv(hydroweb_url)
    hydroweb_df = hydroweb_df.rename(columns={'lake': 'lake_name'})
    hydroweb_id_table = id_table.loc[id_table['source'] == 'hydroweb']
    hydroweb_id_table = hydroweb_id_table.loc[hydroweb_id_table.index.difference(hydroweb_id_table.dropna().index)]
    hydroweb_id_table = hydroweb_id_table.drop(['metadata'], axis=1)
    hydroweb_indexed_df = pd.merge(hydroweb_df, hydroweb_id_table, on='lake_name')
    hydroweb_indexed_df = hydroweb_indexed_df.set_index('id_No')
    hydroweb_json = hydroweb_indexed_df.to_json(orient='index')
    hydroweb_dict = eval(hydroweb_json)

    print('hydroweb metadata prepped')
    # USGS metadata requires use of functions from lake_table_usgs.py, but end result is json dict with unique lake ID
    usgs_df = usgs_tbl.rename(columns={'station_nm': 'lake_name'})
    usgs_id_table = id_table.loc[id_table['source'] == 'usgs']
    usgs_id_table = usgs_id_table.loc[usgs_id_table.index.difference(usgs_id_table.dropna().index)]
    usgs_df = pd.merge(usgs_df, usgs_id_table, on='lake_name')
    usgs_df = usgs_df.set_index('id_No')
    usgs_df = usgs_df.drop(['metadata'], axis=1)
    usgs_dict = usgs_df.to_json(orient='index')
    usgs_dict = usgs_dict.replace('true', '"true"')
    usgs_dict = usgs_dict.replace('false', '"false"')
    usgs_dict = usgs_dict.replace('null', '"null"')
    usgs_dict = eval(usgs_dict)
    print('USGS metadata prepped')



    cursor = connection.cursor()
    # Execute mysql commands
    sql_command = u"UPDATE `reference_ID` SET `metadata` = (%s) WHERE `id_No` = (%s);"

    if len(grealm_dict.values()) > 0:
        printProgressBar(0, len(grealm_dict.values()), prefix='G-REALM:', suffix='Complete', length=50)
        for count, (key, value) in enumerate(grealm_dict.items(), 1):
            cursor.execute(sql_command, (json.dumps(value), key))
            printProgressBar(count + 1, len(grealm_dict.values()), prefix='GREALM-USDA:', suffix='Complete', length=50)
        connection.commit()

    if len(hydroweb_dict.values()) > 0:
        printProgressBar(0, len(hydroweb_dict.values()), prefix='HydroWeb:', suffix='Complete', length=50)
        for count, (key, value) in enumerate(hydroweb_dict.items(), 1):
            cursor.execute(sql_command, (json.dumps(value), key))
            printProgressBar(count + 1, len(hydroweb_dict.values()), prefix='HydroWeb:', suffix='Complete', length=50)
        connection.commit()

    if len(usgs_dict.values()) > 0:
        printProgressBar(0, len(usgs_dict.values()), prefix='USGS-NWIS:', suffix='Complete', length=50)
        for count, (key, value) in enumerate(usgs_dict.items(), 1):
            cursor.execute(sql_command, (json.dumps(value), key))
            printProgressBar(count + 1, len(usgs_dict.values()), prefix='USGS-NWIS:', suffix='Complete', length=50)
        connection.commit()

    connection.close()
    sql_engine.close()

def reference_table_metadata_json_replace(lake_reference_df):
    """
    Update lake metadata within the Reference ID table
    :param usgs_table: dataframe
    :return:
    """
    import pandas as pd
    from sqlalchemy import create_engine
    from utiils import get_ref_table
    from utiils import printProgressBar
    import pymysql
    import json
    import config

    username = config.username
    password = config.password

    # Create database connection engines and cursor
    sql_engine = create_engine('mysql+pymysql://' + username + ':' + password +
                               '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()
    connection = pymysql.connect(host='lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com',
                                 user=username,
                                 password=password,
                                 db='laketest')
    cursor = connection.cursor()

    # Read in reference table for unique Lake ID and Lake name
    id_table = lake_reference_df
    print(lake_reference_df)

    # Read in grealm summary table and clean dataframe
    grealm_url = 'https://ipad.fas.usda.gov/lakes/images/LakesReservoirsCSV.txt'
    grealm_source_df = pd.read_csv(grealm_url, skiprows=3, sep="\t", header=0, parse_dates=[-1],
                                   infer_datetime_format=True, error_bad_lines=False, skip_blank_lines=True)
    grealm_source_df = grealm_source_df[~grealm_source_df['Lake ID'].str.contains("Total")]
    grealm_source_df = grealm_source_df.rename(columns={'Name': 'lake_name', 'Lake ID': 'grealm_database_ID'})

    # Rename lake name to <lake name>_<Resolution> if 2 or more versions of the lake exist
    grealm_source_df.loc[grealm_source_df.lake_name.duplicated(keep=False), 'lake_name'] =\
        grealm_source_df.loc[grealm_source_df.lake_name.duplicated(keep=False), 'lake_name'] + '_' +\
        grealm_source_df.loc[grealm_source_df.lake_name.duplicated(keep=False), 'Resolution'].astype(str)

    # Merge reference and grealm tables while keeping unique lake ID number from db, convert to json dict
    grealm_id_table = id_table[(id_table['source'] == 'grealm')]
    grealm_id_table = grealm_id_table.loc[grealm_id_table.index.difference(grealm_id_table.dropna().index)]
    #grealm_id_table = grealm_id_table.drop(['metadata'], axis=1)

    df_grealm = grealm_id_table.merge(grealm_source_df, on='lake_name', how='inner')
    print(df_grealm.columns)
    df_grealm = df_grealm.set_index('id_No')
    grealm_json = df_grealm.to_json(orient='index')
    try:
        grealm_dict = eval(grealm_json)
    except NameError:
        grealm_dict = {}

    print('grealm metadata prepped')

    # repeat process with hydroweb summary table, results in json dict with Unique lake ID
    hydroweb_url = 'http://hydroweb.theia-land.fr/hydroweb/authdownload?list=lakes&format=txt'
    hydroweb_df = pd.read_csv(hydroweb_url)
    hydroweb_df = hydroweb_df.rename(columns={'lake': 'lake_name'})
    hydroweb_id_table = id_table.loc[id_table['source'] == 'hydroweb']
    hydroweb_id_table = hydroweb_id_table.loc[hydroweb_id_table.index.difference(hydroweb_id_table.dropna().index)]
    #hydroweb_id_table = hydroweb_id_table.drop(['metadata'], axis=1)
    hydroweb_indexed_df = pd.merge(hydroweb_df, hydroweb_id_table, on='lake_name')
    hydroweb_indexed_df = hydroweb_indexed_df.set_index('id_No')
    hydroweb_json = hydroweb_indexed_df.to_json(orient='index')
    hydroweb_dict = eval(hydroweb_json)

    print('hydroweb metadata prepped')
    # USGS metadata requires use of functions from lake_table_usgs.py, but end result is json dict with unique lake ID
    usgs_df = id_table.loc[id_table['source'] == 'usgs']
    usgs_df = usgs_df.rename(columns={'station_nm': 'lake_name'})
    usgs_id_table = id_table.loc[id_table['source'] == 'usgs']
    usgs_id_table = usgs_id_table.loc[usgs_id_table.index.difference(usgs_id_table.dropna().index)]
    usgs_df = pd.merge(usgs_df, usgs_id_table, on='lake_name')
    usgs_df = usgs_df.set_index('id_No')
    #usgs_df = usgs_df.drop(['metadata'], axis=1)
    usgs_dict = usgs_df.to_json(orient='index')
    usgs_dict = usgs_dict.replace('true', '"true"')
    usgs_dict = usgs_dict.replace('false', '"false"')
    usgs_dict = usgs_dict.replace('null', '"null"')
    usgs_dict = eval(usgs_dict)
    print('USGS metadata prepped')


    print(usgs_dict)
    print(grealm_dict)
    print(hydroweb_dict)
    cursor = connection.cursor()
    # Execute mysql commands
    sql_command = u"UPDATE `reference_ID` SET `metadata` = (%s) WHERE `id_No` = (%s);"

    if len(grealm_dict.values()) > 0:
        printProgressBar(0, len(grealm_dict.values()), prefix='G-REALM:', suffix='Complete', length=50)
        for count, (key, value) in enumerate(grealm_dict.items(), 1):
            cursor.execute(sql_command, (json.dumps(value), key))
            printProgressBar(count + 1, len(grealm_dict.values()), prefix='GREALM-USDA:', suffix='Complete', length=50)
        connection.commit()

    if len(hydroweb_dict.values()) > 0:
        printProgressBar(0, len(hydroweb_dict.values()), prefix='HydroWeb:', suffix='Complete', length=50)
        for count, (key, value) in enumerate(hydroweb_dict.items(), 1):
            cursor.execute(sql_command, (json.dumps(value), key))
            printProgressBar(count + 1, len(hydroweb_dict.values()), prefix='HydroWeb:', suffix='Complete', length=50)
        connection.commit()

    if len(usgs_dict.values()) > 0:
        printProgressBar(0, len(usgs_dict.values()), prefix='USGS-NWIS:', suffix='Complete', length=50)
        for count, (key, value) in enumerate(usgs_dict.items(), 1):
            cursor.execute(sql_command, (json.dumps(value), key))
            printProgressBar(count + 1, len(usgs_dict.values()), prefix='USGS-NWIS:', suffix='Complete', length=50)
        connection.commit()

    connection.close()
    sql_engine.close()



