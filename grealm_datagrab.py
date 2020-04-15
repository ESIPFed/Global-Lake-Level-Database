def grealm_datagrab():
    """
    Accesses the grealm website and requests a summary table of lake information to use in grealm_data_multindex
    :return: df of summary information from grealm website
    """
    import urllib3
    import certifi
    import pandas as pd

    target_url = 'https://ipad.fas.usda.gov/lakes/images/summary2.txt'

    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

    req = http.request('GET', target_url)
    text = req.data.decode()
    text_by_line = text.split('\n')

    headers = ['site_id', 'site_name', 'Latitude', 'Longitude', 'YYYYMMDD',
               'Hr', 'Min', 'Ht(m)', 'Sigma(m)', 'Updated_on', 'Type']
    df = pd.DataFrame([i.split() for i in text_by_line[1:]], columns=headers)

    df = df.dropna()

    df['date'] = df['YYYYMMDD'] + ' ' + df['Hr'] + df['Min']
    df = df.drop(['YYYYMMDD', 'Hr', 'Min'], axis=1)
    pd.to_datetime(df['date'], format='%Y%m%d %H%M')

    return df

