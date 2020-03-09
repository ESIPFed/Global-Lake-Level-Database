def url_reader():
    import urllib3
    import pandas as pd

    target_url = 'https://ipad.fas.usda.gov/lakes/images/summary2.txt'

    http = urllib3.PoolManager()

    req = http.request('GET', target_url)
    text = req.data.decode()
    text_by_line = text.split('\n')

    df = pd.DataFrame([i.split() for i in text_by_line[1:]], columns=None)
    headers = ['Lake_#', 'Lake_Name', 'Latitude', 'Longitude', 'YYYYMMDD',
               'Hr', 'Min', 'Ht(m)', 'Sigma(m)', 'Updated_on']
    df.columns = headers
    df = df.dropna()
    return df

