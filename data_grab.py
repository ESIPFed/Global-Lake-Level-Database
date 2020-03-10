def data_grab(df):
    import os
    from pathlib import Path
    import certifi
    import urllib3
    import pandas as pd

    data_folder = Path('/Users/johnfraney/Desktop/ESIP_data')
    if not os.path.exists(data_folder):
        os.mkdir(data_folder)
        print('Directory ', data_folder, ' Created')
    else:
        print('Directory ', data_folder, ' already Exists')

    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

    for id, name in zip(df['Lake_#'], df['Lake_Name']):
        if id == '0012':

            lake_folder = data_folder / '{}_Lake_{}'.format(id, name)
            if not os.path.exists(lake_folder):
                os.mkdir(lake_folder)
                print('Directory ', lake_folder, ' Created')
            else:
                print('Directory ', lake_folder, ' already Exists')

            target_url = 'https://ipad.fas.usda.gov/lakes/images/lake%s.10d.2.txt' % id

            req = http.request('GET', target_url)
            text = req.data.decode()
            text_by_line = text.split('\n')

            header_text = '\n'.join(text_by_line[:49])
            header_information_path = lake_folder / '{}_{}_header_information.txt'.format(id, name)
            print('Writing header information to: ', header_information_path)
            header_information = open(header_information_path, 'w')
            header_information.write(header_text)
            header_information.close()

            df = pd.DataFrame([i.split() for i in text_by_line[50:]], columns = None)
            df_path = lake_folder / '{}_{}_water_level_data.csv'.format(id, name)
            print('writing dataframe to: ', df_path)
            df.to_csv(df_path, index=False)
