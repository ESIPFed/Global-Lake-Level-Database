def hierarchy(df):
    import pandas as pd
    import certifi
    import urllib3

    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
    df_final = pd.DataFrame()
    for ids, name in zip(df['Lake_#'], df['Lake_Name']):
        target_url = 'https://ipad.fas.usda.gov/lakes/images/lake%s.10d.2.txt' % ids

        req = http.request('GET', target_url)
        text = req.data.decode()
        text_by_line = text.split('\n')

        df_temp = pd.DataFrame([i.split() for i in text_by_line[50:]], columns=None)
        df_temp['Lake_ID'] = '{}'.format(ids)
        cols = df_temp.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df_temp = df_temp[cols]
        df_temp.set_index(['Lake_ID', 2], inplace=True)
        df_final = pd.concat([df_final, df_temp])
    return df_final
