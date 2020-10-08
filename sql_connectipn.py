from sqlalchemy import create_engine
import pymysql
import pandas as pd
import json
# TODO: sanitize inputs of apostrophes, these break the MySQL code
# connection = pymysql.connect(host='lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com',
#                              user='***REMOVED***',
#                              password='***REMOVED***',
#                              db='laketest')
#
# cursor = connection.cursor()
#
# sqlEngine = create_engine('mysql+pymysql://***REMOVED***:***REMOVED***'
#                           '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()


def lake_id_table_creation():
    sqlEngine = create_engine('mysql+pymysql://***REMOVED***:***REMOVED***'
                              '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()

    df_hydro_names = pd.read_sql('HYDRO_MTADTA', con=sqlEngine, columns=['lake'])
    df_hydro_names['source'] = 'hydroweb'
    df_hydro_names = df_hydro_names.rename(columns={'lake': 'name'})

    df_usgs_name = pd.read_sql('USGS_MTADTA', con=sqlEngine, columns=['name'])
    df_usgs_name['source'] = 'usgs'

    df_grealm_name = pd.read_sql('GREALM MTADTA', con=sqlEngine, columns=['Name'])
    df_grealm_name['source'] = 'grealm'
    df_grealm_name = df_grealm_name.rename(columns={'Name': 'name'})

    df = pd.concat([df_hydro_names, df_usgs_name, df_grealm_name], ignore_index=True)
    df = df.rename(columns={'name': 'lake_name'})

    df.to_sql('Lake_ID', con=sqlEngine, if_exists='replace', index_label='id_No')

def id_no_labeler():
    sqlEngine = create_engine('mysql+pymysql://***REMOVED***:***REMOVED***'
                              '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()
    connection = pymysql.connect(host='lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com',
                                 user='***REMOVED***',
                                 password='***REMOVED***',
                                 db='laketest')

    cursor = connection.cursor()
    id_table = pd.read_sql('Lake_ID', con=sqlEngine)

    usgs_label = id_table.loc[id_table['source'] == 'usgs']
    hydroweb_label = id_table.loc[id_table['source'] == 'hydroweb']
    grealm_label = id_table.loc[id_table['source'] == 'grealm']
    # hotfix for the error in sql code, this should be cleaned before being written to Lake_ID Table
    grealm_label.loc[1791, 'lake_name'] = "El''ton"
    usgs_label.loc[928, 'lake_name'] = "LAC VIEUX DESERT NEAR LAND O''LAKES, WI"

    cursor.execute("SET innodb_lock_wait_timeout = 2400;")
    # Update GREALM_MTADTA table
    try:
        sql_grealm = "ALTER TABLE GREALM_MTADTA ADD id_No INT NOT NULL FIRST;"
        cursor.execute(sql_grealm)
        print('Creating id_No Column in GREALM_MTADTA')
        print('Updating id_No Column in GREALM_MTADTA')
    except pymysql.err.InternalError:
        print('Updating id_No Column in GREALM_MTADTA')
    for num, name in zip(grealm_label['id_No'], grealm_label['lake_name']):
        cursor.execute(u"UPDATE `GREALM_MTADTA` SET `id_No` = (%s) WHERE `LakeName` = ('%s');", (num, name))
    connection.commit()

    # Update HYDRO_MTADTA table
    try:
        sql_hydro = "ALTER TABLE HYDRO_MTADTA ADD id_No INT NOT NULL FIRST;"
        cursor.execute(sql_hydro)
        print('Creating id_No Column in HYDRO_MTADTA')
        print('Updating id_No Column in HYDRO_MTADTA')
    except pymysql.err.InternalError:
        print('Updating id_No Column in HYDRO_MTADTA')
    for num, name in zip(hydroweb_label['id_No'], hydroweb_label['lake_name']):
        cursor.execute(u"UPDATE `HYDRO_MTADTA` SET `id_No` = (%s) WHERE `lake` = ('%s');", (num, name))
    connection.commit()

    # Update USGS_MTADTA table
    try:
        sql_usgs = "ALTER TABLE USGS_MTADTA ADD id_No INT NOT NULL FIRST;"
        cursor.execute(sql_usgs)
        print('Creating id_No Column in USGS_MTADTA')
        print('Updating id_No Column in USGS_MTADTA')
    except pymysql.err.InternalError:
        print('Updating id_No Column in USGS_MTADTA')
    for num, name in zip(usgs_label['id_No'], usgs_label['lake_name']):
        cursor.execute(u"UPDATE `USGS_MTADTA` SET `id_No` = (%s) WHERE `name` = ('%s');", (num, name))
    connection.commit()

    # Close Database Connection
    connection.close()


# def reference_table_creation():
sqlEngine = create_engine('mysql+pymysql://***REMOVED***:***REMOVED***'
                          '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()
connection = pymysql.connect(host='lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com',
                             user='***REMOVED***',
                             password='***REMOVED***',
                             db='laketest')
cursor = connection.cursor()
reference_table = pd.read_sql('Lake_ID', con=sqlEngine)

cursor.execute("SELECT JSON_ARRAYAGG(JSON_OBJECT('id_No', `id_No`, 'grealm_database_ID', `Grealm_database_ID`,"
               "'lakeName', `LakeName`, 'continent', `Continent`, 'country', `Country`, 'type', `Type`, 'resolution',"
               "`Resolution`, 'Satellite_observation_period', `Satellite Observation Period`)) from GREALM_MTADTA")
grealm_mtadta = cursor.fetchone()
grealm_mtadta = grealm_mtadta[0][1:-1]

json_list = []
dec = json.JSONDecoder()
pos = 0
while not pos == len(grealm_mtadta):
    print(pos)
    j, json_len = dec.raw_decode(s=grealm_mtadta[pos:])
    print(j)
    json_list.append(j)
    pos += (json_len + 2)
    print(pos)



# if __name__ == "__main__":
#     id_no_labeler()
