def update_sql(df, name):
    import pymysql
    import pandas as pd
    from os import environ
    from sqlalchemy import create_engine
    environ['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://flask:flasktest@flask-db-identifier.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/db_name'

    db_URI = environ.get('SQLALCHEMY_DATABASE_URI')
    engine = create_engine(db_URI)
    df.to_sql(name, engine, if_exists='replace', chunksize=500, index=False)