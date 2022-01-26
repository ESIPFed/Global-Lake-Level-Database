# %% Section: MetaInfo
__author__ = ['John Franey', 'Jake Gearon']
__credits__ = ['John Franey', 'Jake Gearon', 'Earth Science Information Partners (ESIP)']
__version__ = '1.0.0'
__maintainer__ = 'John Franey'
__email__ = 'franeyjohn96@gmail.com'
__status__ = 'Development'
def replace_reference_id_table():
    """
    Gathers all lake names from data sources and assigns a unique ID number
    _This should only be used on initial DB creation_
    otherwise use update_reference_id_table()
    :return: None
    """

    import pandas as pd
    from sqlalchemy import create_engine
    from lake_table_usgs import update_usgs_lake_names
    import config

    username = config.username
    password = config.password

    confirmation = input('Are you sure you want to replace the entire database?\nType "yes" to continue: ')
    if confirmation != 'yes':
        print('Confirmation not valid. No action taken')
    else:
        # Create database connection engines and cursor
        sql_engine = create_engine('mysql+pymysql://' + username + ':' + password +
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
        usgs_source_df_raw = update_usgs_lake_names()

        usgs_df = usgs_source_df_raw.rename(columns={'station_nm': 'lake_name'})
        usgs_df = usgs_df.filter(['lake_name'])
        usgs_df.insert(0, 'source', 'usgs')

        # Get lake names from grealm, drop metadata, add source info
        print('--------Gathering G-Realm lake information--------')
        grealm_url = 'https://ipad.fas.usda.gov/lakes/images/LakesReservoirsCSV.txt'
        grealm_df = pd.read_csv(grealm_url, skiprows=3, sep="\t", header=0, parse_dates=[-1],
                                infer_datetime_format=True, on_bad_lines='skip', skip_blank_lines=True)
        grealm_df = grealm_df[~grealm_df['Lake ID'].str.contains("Total")]
        grealm_df = grealm_df.rename(columns={'Name': 'lake_name'})
        grealm_df.loc[grealm_df.lake_name.duplicated(keep=False), 'lake_name'] = \
            grealm_df.loc[grealm_df.lake_name.duplicated(keep=False), 'lake_name'] + '_' + \
            grealm_df.loc[grealm_df.lake_name.duplicated(keep=False), 'Resolution'].astype(str)
        grealm_df = grealm_df.filter(['lake_name'])
        grealm_df.insert(0, 'source', 'grealm')

        lake_reference_df = pd.concat([hydroweb_df, usgs_df, grealm_df], ignore_index=True)

        print('--------Overwriting database--------')
        lake_reference_df.to_sql('reference_ID', con=sql_engine, if_exists='replace', index_label='id_No')
        return usgs_source_df_raw


def update_reference_id_table():
    """
    Create DataFrame of lake names to pass to updater function
    :return: Pandas DataFrame of lakes to be updated
    """
    import pandas as pd
    from sqlalchemy import create_engine
    from utiils import get_ref_table
    from lake_table_usgs import get_usgs_sites
    from lake_table_usgs import update_usgs_lake_names
    import pymysql
    import config

    username = config.username
    password = config.password

    # Create database connection
    connection = pymysql.connect(host='lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest',
                                 user=username,
                                 password=password,
                                 db='laketest')
    cursor = connection.cursor()

    reference_table = get_ref_table()
    reference_table = reference_table.drop('metadata', axis=1)
    print('Reference Table Read')

    grealm_url = 'https://ipad.fas.usda.gov/lakes/images/LakesReservoirsCSV.txt'
    grealm_source_df = pd.read_csv(grealm_url, skiprows=3, sep="\t", header=0, parse_dates=[-1],
                                   infer_datetime_format=True, on_bad_lines='skip', skip_blank_lines=True)
    grealm_source_df = grealm_source_df[~grealm_source_df['Lake ID'].str.contains("Total")]
    grealm_source_df = grealm_source_df.rename(columns={'Name': 'lake_name'})
    grealm_source_df.loc[grealm_source_df.lake_name.duplicated(keep=False), 'lake_name'] = \
        grealm_source_df.loc[grealm_source_df.lake_name.duplicated(keep=False), 'lake_name'] + '_' + \
        grealm_source_df.loc[grealm_source_df.lake_name.duplicated(keep=False), 'Resolution'].astype(str)
    grealm_source_df = grealm_source_df.filter(['lake_name'])


    # Merge reference and grealm tables while keeping unique lake ID number from db, convert to json dict
    grealm_existing_lakes_table = reference_table.loc[reference_table['source'] == 'grealm']
    grealm_ready_df = grealm_source_df[~grealm_source_df.lake_name.isin(grealm_existing_lakes_table['lake_name'])]
    grealm_ready_df.insert(0, 'source', 'grealm')
    print("New GREALM-USDA Lakes ready for insertion")

    # Grab hydroweb source data
    hydroweb_url = 'http://hydroweb.theia-land.fr/hydroweb/authdownload?list=lakes&format=txt'
    hydroweb_source_df = pd.read_csv(hydroweb_url)
    hydroweb_source_df = hydroweb_source_df.rename(columns={'lake': 'lake_name'})
    hydroweb_source_df = hydroweb_source_df.filter(['lake_name'])

    hydroweb_existing_lake_df = reference_table.loc[reference_table['source'] == 'hydroweb']
    hydroweb_ready_df = hydroweb_source_df[~hydroweb_source_df.lake_name.isin(hydroweb_existing_lake_df['lake_name'])]
    hydroweb_ready_df.insert(0, 'source', 'hydroweb')
    print("New HydroWeb Lakes ready for insertion")

    # Grab usgs source data
    usgs_source_df_raw = update_usgs_lake_names()
    usgs_source_df = usgs_source_df_raw.rename(columns={'station_nm': 'lake_name'})
    usgs_source_df = usgs_source_df.filter(['lake_name'])
    usgs_existing_lake_df = reference_table.loc[reference_table['source'] == 'usgs']
    # TODO: Lake names are not copying over
    usgs_ready_df = usgs_source_df[~usgs_source_df.lake_name.isin(usgs_existing_lake_df['lake_name'])]
    usgs_ready_df.insert(0, 'source', 'usgs')
    print("New USGS-NWIS Lakes ready for insertion")

    sql_ready_df = pd.concat([grealm_ready_df, hydroweb_ready_df, usgs_ready_df], ignore_index=True)

    sql_command = "INSERT IGNORE INTO reference_ID(lake_name, `source`) VALUES (%s, %s);"
    for name, source in zip(sql_ready_df['lake_name'], sql_ready_df['source']):
        cursor.execute(sql_command, (name, source))
    connection.commit()
    connection.close()
    return usgs_source_df_raw
