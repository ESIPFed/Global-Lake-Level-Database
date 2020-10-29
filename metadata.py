def reference_table_mtadta_json():

    import pandas as pd
    from sqlalchemy import create_engine
    import pymysql
    from usgs_datagrab import get_usgs_sites
    from usgs_datagrab import update_usgs_meta
    import json

    # Create database connection engines and cursor
    sql_engine = create_engine('mysql+pymysql://***REMOVED***:***REMOVED***'
                               '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()
    connection = pymysql.connect(host='lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com',
                                 user='***REMOVED***',
                                 password='***REMOVED***',
                                 db='laketest')
    cursor = connection.cursor()

    # Read in reference table for unique Lake ID and Lake name
    id_table = pd.read_sql('reference_ID', con=sql_engine, columns=['id_No', 'lake_name', 'source', 'metadata'])
    print('Reference Table Read')

    # Read in grealm summary table and clean dataframe
    grealm_url = 'https://ipad.fas.usda.gov/lakes/images/LakesReservoirsCSV.txt'
    grealm_source_df = pd.read_csv(grealm_url, skiprows=3, sep="\t", header=0, parse_dates=[-1],
                     infer_datetime_format=True, error_bad_lines=False, skip_blank_lines=True)
    grealm_source_df = grealm_source_df[~grealm_source_df['Lake ID'].str.contains("Total")]
    grealm_source_df = grealm_source_df.rename(columns={'Name': 'lake_name', 'Lake ID': 'grealm_database_ID'})

    # Merge reference and grealm tables while keeping unique lake ID number from db, convert to json dict
    grealm_id_table = id_table[(id_table['source'] == 'grealm')]
    grealm_id_table = grealm_id_table.loc[grealm_id_table.index.difference(grealm_id_table.dropna().index)]
    grealm_id_table = grealm_id_table.drop(['metadata'], axis=1)
    df_grealm = pd.merge(grealm_source_df, grealm_id_table, on='lake_name')
    df_grealm = df_grealm.set_index('id_No')
    grealm_json = df_grealm.to_json(orient='index')
    grealm_dict = eval(grealm_json)

    print('grealm dictionary created')

    # repeat process with hydroweb summary table, results in json dict with Unique lake ID
    hydroweb_url = 'http://hydroweb.theia-land.fr/hydroweb/authdownload?list=lakes&format=txt'
    hydroweb_df = pd.read_csv(hydroweb_url)
    hydroweb_df = hydroweb_df.rename(columns={'lake': 'lake_name'})
    hydroweb_id_table = id_table.loc[id_table['source'] == 'hydroweb']
    hydroweb_id_table = hydroweb_id_table.loc[hydroweb_id_table.index.difference(hydroweb_id_table.dropna().index)]
    hydroweb_indexed_df = pd.merge(hydroweb_df, hydroweb_id_table, on='lake_name')
    hydroweb_indexed_df = hydroweb_indexed_df.set_index('id_No')
    hydroweb_json = hydroweb_indexed_df.to_json(orient='index')
    hydroweb_dict = eval(hydroweb_json)

    print('hydroweb dictionary created')
    # USGS metadata requires use of functions from usgs_datagrab.py, but end result is json dict with unique lake ID
    usgs_df = update_usgs_meta(get_usgs_sites())
    usgs_df = usgs_df.rename(columns={'name': 'lake_name'})
    usgs_id_table = id_table.loc[id_table['source'] == 'usgs']
    usgs_id_table = usgs_id_table.loc[usgs_id_table.index.difference(usgs_id_table.dropna().index)]
    usgs_df = pd.merge(usgs_df, usgs_id_table, on='lake_name')
    usgs_df = usgs_df.set_index('id_No')
    usgs_df = usgs_df.drop(['metadata'], axis=1)
    usgs_dict = usgs_df.to_json(orient='index')
    usgs_dict = usgs_dict.replace('true', '"true"')
    usgs_dict = usgs_dict.replace('false', '"false"')
    usgs_dict = eval(usgs_dict)

    # USGS process is slow, while in dev I am accessing usgs_json from saved file
    # with open('/Users/johnfraney/Desktop/usgs_json.json') as f:
    #     usgs_dict = json.load(f)

    # Execute mysql commands, could possibly merge dicts? have not tried, see below line. 1 loop for each dict
    # merged_dict = {**hydroweb_dict, **usgs_dict, **grealm_dict}
    print('----------UPDATING GREALM METADATA----------')
    sql_command = u"UPDATE `reference_ID` SET `metadata` = (%s) WHERE `id_No` = (%s);"
    for key, value in grealm_dict.items():
        print(key, value['lake_name'])      # Just to keep track of progress in console view
        cursor.execute(sql_command, (json.dumps(value), key))
    connection.commit()

    print('----------UPDATING HYDROWEB METADATA----------')
    for key, value in hydroweb_dict.items():
        print(key, value['lake_name'])      # Just to keep track of progress in console view
        cursor.execute(sql_command, (json.dumps(value), key))
    connection.commit()

    print('----------UPDATING USGS METADATA----------')
    for key, value in usgs_dict.items():
        print(key, value['lake_name'])      # Just to keep track of progress in console view
        cursor.execute(sql_command, (json.dumps(value), key))
    connection.commit()
    connection.close()

    print('task complete')

if __name__ == '__main__':
    reference_table_mtadta_json()

