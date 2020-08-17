
import requests
from db_create import update_sql
import pandas as pd
import certifi
import urllib3
def update_grealm_meta():
    url = 'https://ipad.fas.usda.gov/lakes/images/LakesReservoirsCSV.txt'
    df = pd.read_csv(url, skiprows=3, sep="\t", header=0, parse_dates=[-1],
                     infer_datetime_format=True, error_bad_lines=False, skip_blank_lines=True)
    df = df[~df['Lake ID'].str.contains("Total")]
    # df[['Start_Date', 'End_Date']] = df['Satellite Observation Period'].\
    #     str.split(["[,-]"], expand=True)
    update_sql(df, 'GREALM MTADTA')

    # import requests
    # from requests.adapters import HTTPAdapter
    # from requests.packages.urllib3.util.retry import Retry
    #
    # session = requests.Session()
    # retry = Retry(connect=3, backoff_factor=0.5)
    # adapter = HTTPAdapter(max_retries=retry)
    # session.mount('http://', adapter)
    # session.mount('https://', adapter)
    #
    # req = session.get(url)
    #
    # # df = pd.read_csv('https://ipad.fas.usda.gov/lakes/images/LakesReservoirsCSV.txt',
    # #                  skiprows=3, delim_whitespace=True,
    # #                  names = ["Lake ID","Name","Continent","Country","Type",
    # #                         "Resolution","Satellite", "Observation", "Period"])
    # update_sql(df, 'GREALM MTADTA')


def grealm_data_multindex():
    """
    Uses summary df from grealm_datagrab to access web pages from grealm of all available lakes

    :return: multindex dataframe of all vaialbe lakes from grealm website
    """
    import pandas as pd
    import urllib3
    import certifi

    headers = ['site_id', 'site_name', 'Latitude', 'Longitude', 'YYYYMMDD',
               'Hr', 'Min', 'Ht(m)', 'Sigma(m)', 'Updated_on', 'Type']
    df = pd.read_csv('https://ipad.fas.usda.gov/lakes/images/summary2.txt',
                     delim_whitespace=True, skiprows=1, names=headers
                     )

    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
    df_dict = {}

    for ids, name in zip(df['site_id'], df['site_name']):
        target_url = 'https://ipad.fas.usda.gov/lakes/images/lake{}.10d.2.txt'.format(ids)
        req = http.request("GET", target_url)
        text = req.data.decode()
        text_by_line = text.split('\n')
        df = pd.DataFrame([i.split() for i in text_by_line[50:]], columns=None)
        df_dict.update({name: df})
    print(len(df_dict))

    grealm_database = pd.concat(df_dict)
    print(grealm_database)
    return grealm_database
