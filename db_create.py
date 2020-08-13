import pymysql
from os import environ
from sqlalchemy import create_engine
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def update_sql(df, name):
    """
    :param df: dataframe to upload
    :param name:
    :return:
    """
    #['SQLALCHEMY_DATABASE_URI'] = 'mysql://{master username}:{db password}@{endpoint}/{db instance name}'

    environ['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://admin:aurs8kah.JAIP6tref@lake-test1.cevt7olsswvw.us-east-2' \
                                         '.rds.amazonaws.com:3306/laketest'
    db_URI = environ.get('SQLALCHEMY_DATABASE_URI')
    engine = create_engine(db_URI)

    df.to_sql(name, engine, if_exists='replace', chunksize=500, index=False)




def requests_retry_session(
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
