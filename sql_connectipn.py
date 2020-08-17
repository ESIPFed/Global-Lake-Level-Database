from sqlalchemy import create_engine
import pymysql
import pandas as pd

connection = pymysql.connect(host='lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com',
                             user='***REMOVED***',
                             password='***REMOVED***',
                             db='laketest')

cursor = connection.cursor()
#
# engine = create_engine()


sqlEngine = create_engine('mysql+pymysql://***REMOVED***:***REMOVED***'
                          '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()


# df.to_sql('<table_name>', con=sqlEngine, if_exists='append')


def lake_id_creation():
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
    id_table = pd.read_sql('Lake_ID', con=sqlEngine)

    usgs_label = id_table.loc[id_table['source'] == 'usgs']
    hydroweb_label = id_table.loc[id_table['source'] == 'hydroweb']
    grealm_label = id_table.loc[id_table['source'] == 'grealm']




id_table = pd.read_sql('Lake_ID', con=sqlEngine)

usgs_label = id_table.loc[id_table['source'] == 'usgs']
hydroweb_label = id_table.loc[id_table['source'] == 'hydroweb']
grealm_label = id_table.loc[id_table['source'] == 'grealm']
#
sql1_grealm = "ALTER TABLE GREALM_MTADTA ADD id_No INT NOT NULL FIRST;"
cursor.execute(sql1_grealm)
for elem in grealm_label['id_No']:
    cursor.execute(u"INSERT INTO `GREALM_MTADTA`(`id_No`) VALUES (%s)" % elem)
    connection.commit()
# # TODO: add underscore to GREALM MTADTA

# lst = [[item] for item in grealm_label['id_No'].tolist()]
# cursor.executemany(u"INSERT INTO `GREALM_MTADTA`(`id_No`) VALUES (%s)", lst)
# connection.commit()

# sql1_hydroweb = "ALTER TABLE HYDRO_MTADTA ADD id_No INT NOT NULL;"
# sql2_hydroweb = "UPDATE HYDRO_MTADTA SET id_No = hydroweb_label['id_No'];"
#
# sql1_usgs = "ALTER TABLE USGS_MTADTA ADD id_No INT NULL;"
# sql2_usgs = "UPDATE USGS_MTADTA SET id_No = usgs_label['id_No'];"
#
# # commands = [sql1_grealm, sql2_grealm, sql1_hydroweb, sql2_hydroweb, sql1_usgs, sql2_usgs]
