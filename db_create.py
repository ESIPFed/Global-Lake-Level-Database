def create_tables():
    import pymysql
    import config

    username = config.username
    password = config.password
    connection = pymysql.connect(host='lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com',
                                 user=username,
                                 password=password,
                                 db='laketest')
    cursor = connection.cursor()

    reference_table_sql_query = "CREATE TABLE IF NOT EXISTS reference_ID(" \
                                "id_No INT NOT NULL AUTO_INCREMENT PRIMARY KEY," \
                                "lake_name TEXT," \
                                "source TEXT," \
                                "metadata JSON" \
                                ")"

    lake_water_level_query = "CREATE TABLE IF NOT EXISTS lake_water_level(" \
                             "id_No INT," \
                             "`date` DATE," \
                             "lake_name TEXT," \
                             "water_level FLOAT," \
                             "PRIMARY KEY (id_No, `date`)" \
                             ")"

    cursor.execute(reference_table_sql_query)
    cursor.execute(lake_water_level_query)
    connection.commit()
    connection.close()

