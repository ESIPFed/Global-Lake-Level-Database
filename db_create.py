def update_sql(df, name):
    import pymysql
    import pandas as pd
    from os import environ
    from sqlalchemy import create_engine
    environ['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://***REMOVED***:lake-test1@lake-test1.cevt7olsswvw.us-east-2' \
                                         '.rds.amazonaws.com:3306/laketest'
    #['SQLALCHEMY_DATABASE_URI'] = 'mysql://{master username}:{db password}@{endpoint}/{db instance name}'
    db_URI = environ.get('SQLALCHEMY_DATABASE_URI')
    engine = create_engine(db_URI)
    df.to_sql(name, engine, if_exists='replace', chunksize=500, index=False)
