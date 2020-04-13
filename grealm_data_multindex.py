def grealm_data_multindex():
    """
    Uses summary df from grealm_datagrab to access web pages from grealm of all available lakes

    :return: multindex dataframe of all vaialbe lakes from grealm website
    """
    from grealm_datagrab import grealm_datagrab
    import  pandas as pd
    import urllib3
    import certifi

    df = grealm_datagrab()

    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
    df_dict = {}
    for ids, name in zip(df['site_id'], df['site_name']):
        target_url = 'https://ipad.fas.usda.gov/lakes/images/lake{}.10d.2.txt'.format(ids)

        req = http.request("GET", target_url)
        text = req.data.decode()
        text_by_line = text.split('\n')
        df = pd.DataFrame([i.split() for i in text_by_line[50:]], columns=None)
        df_dict.update({name : df})

    grealm_database = pd.concat(df_dict)
    print(grealm_database)
    return grealm_database

