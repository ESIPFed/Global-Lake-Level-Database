# Database scraper for HydroWeb
# 04/9/20
# Jake Gearon for ESIP Grant 2020
import requests
import pandas as pd
from bs4 import BeautifulSoup
from db_create import update_sql
# TODO: figure out best practice for username / hydroweb account


def update_hydro_meta():
    target_url = 'http://hydroweb.theia-land.fr/hydroweb/authdownload?list=lakes&format=txt'
    df = pd.read_csv(target_url)
    update_sql(df, "HYDRO_MTADTA")



def hydro_grab():
    """
    An updating function that scrapes all available data from HydroWeb and returns it as a multi-index dataframe
    :return: multi-index dataframe of lake data
    """
    from io import BytesIO
    from zipfile import ZipFile
    import urllib.request
    import pandas as pd

    df_dict = {}
    target_url = urllib.request.urlopen('http://hydroweb.theia-land.fr/hydroweb/authdownload?products=lakes&'
                                        'user=jake.gearon@gmail.com&pwd=vHs98NdXe9BxWNyok*Pp&format=txt')
    with ZipFile(BytesIO(target_url.read())) as my_zip_file:
        for contained_file in my_zip_file.namelist():
            name = contained_file[11:-4]
            df = pd.read_csv(my_zip_file.open(contained_file),
                             sep=';',
                             comment='#',
                             skiprows=1,
                             index_col=False,
                             names=['Decimal Year', 'Date | yyyy/mm/dd', 'Time | hh:mm',
                                    'Height above surface of ref (m)', 'Standard deviation from height (m)',
                                    'Area (km2)', 'Volume with respect to volume of first date (km3)', 'Flag'])
            df_dict.update({'Lake '+ name : df})
            break
    hydroweb_df = pd.concat(df_dict)
    return hydroweb_df

if __name__ == '__main__':
    df = hydro_grab()
    #df = update_hydro_meta()