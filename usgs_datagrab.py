import io

# def usgs_datagrab():
import lxml
import xml.etree.cElementTree as cET

"""
usgs_datagrab accesses the USGS NWIS website and scraps the water surface level above NAVD 88 for available Lakes

:return: pandas multindex dataframe of lake name, USGS site ID, date of data acquisition, and associated water level
"""
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd

begin_date = '1838-01-01'
now = datetime.now()
end_date = now.strftime('%Y-%m-%d')
# TODO Document what each parameter means or have a link to USGS website explaining
param_list = ['72020','99020','30211','00062','99065',
              '30207','00065','62600','72292','62615','62614','62616',
              '62617','72214','72264','72275','72333','72335','72336',
              '72199','72178']

lake_list = 'https://waterdata.usgs.gov/nwis/dv?referred_module=sw&' \
            'site_tp_cd=LK&index_pmcode_72020=1&' \
            'index_pmcode_99020=1&' \
            'index_pmcode_30211=1&' \
            'index_pmcode_00062=1&' \
            'index_pmcode_99065=1&' \
            'index_pmcode_30207=1&' \
            'index_pmcode_00065=1&' \
            'index_pmcode_62600=1&' \
            'index_pmcode_72292=1&' \
            'index_pmcode_72293=1&' \
            'index_pmcode_62615=1&' \
            'index_pmcode_62614=1&' \
            'index_pmcode_62616=1&' \
            'index_pmcode_62617=1&' \
            'index_pmcode_72214=1&' \
            'index_pmcode_72264=1&' \
            'index_pmcode_72275=1&' \
            'index_pmcode_72333=1&' \
            'index_pmcode_72335=1&' \
            'index_pmcode_72336=1&' \
            'index_pmcode_72199=1&' \
            'index_pmcode_72178=1&' \
            'group_key=NONE&format=sitefile_output&sitefile_output_format=xml&' \
            'column_name=site_no&column_name=station_nm&range_selection=date_range&' \
            'begin_date=1838-01-01&end_date=2020-06-16&date_format=YYYY-MM-DD&rdb_compression=file&' \
            'list_of_search_criteria=site_tp_cd%2Crealtime_parameter_selection'

def get_usgs_sites():
    global site_list
    #todo create dictionary of parameter id and names
    lake_document = requests.get(lake_list)
    lakesoup = BeautifulSoup(lake_document.content, "lxml-xml")
    site_no_list = [i.text for i in lakesoup.find_all("site_no")]
    site_list = []
    [site_list.append(i) for i in site_no_list]
    print("{} sites indexed".format(len(site_list)))
    return site_list

def update_usgs_database(site_list):
    global soup
    if isinstance(site_list, list) is True:
        for i in site_list:
            target_url = 'http://waterservices.usgs.gov/nwis/dv/?sites={}&siteType=LK&startDT={}&endDT={}' \
                         '&format=waterml&variable=00062,' \
                         '30211,62600,62614,62615,62616,62617,62618,72020,' \
                         '72292,72293,72333,99020,72178,72199'.format(i, begin_date, end_date).replace("%2C", ",")
            req = requests.get(target_url)
            soup = BeautifulSoup(req.content, "lxml-xml")
            df_rows = []
            df = pd.DataFrame(columns=['Name', 'Date', 'Water Level [m]', 'Qualifiers'])
            for elem in soup.find_all("value"):
                name = elem.find("")
                val = (elem.find("value").get_text())
                dt = elem.find("value").attrs['dateTime']
                qls = elem.find("value").attrs['qualifiers']

    elif isinstance(site_list, str) is True:
        target_url = 'http://waterservices.usgs.gov/nwis/dv/?sites={}&siteType=LK&startDT={}&endDT={}' \
                     '&format=waterml&variable=00062,' \
                     '30211,62600,62614,62615,62616,62617,62618,72020,' \
                     '72292,72293,72333,99020,72178,72199'.format(site_list, begin_date, end_date).replace("%2C", ",")
        print("url", target_url)
        req = requests.get(target_url)
        soup = BeautifulSoup(req.content, "lxml-xml")
        df_rows = []
        df = pd.DataFrame(columns=['Name','Date', 'Water Level [m]', 'Qualifiers'])
        for elem in soup.find_all("value"):
            name = elem.find("")
            val = (elem.find("value").get_text())
            dt = elem.find("value").attrs['dateTime']
            qls = elem.find("value").attrs['qualifiers']

    else:
        print("site_list must be a single str or list of str")
    # try:
    #     return soup
    # except Exception as e:

    #     print(type(e))

if __name__ == "__main__":
    s = get_usgs_sites()[0]
    update_usgs_database(s)









    #
    # f = open(resource_file('target_url'), 'rb').read()
    #
    # sites = wml(f).response

    # http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
    # req = http.request('GET', target_url)
    # text = req.data.decode()
    #
    # retrieved_date = re.search(r"retrieved:.*$", text, re.M)
    # print('This data was accessed on: {}'.format(retrieved_date.group()))
    #
    # site_ids = re.findall(r"USGS (\d*) (.*)$", text, re.M)
    # temp_column_names = ['site_id', 'site_name']
    # name_df = pd.DataFrame(site_ids, columns=temp_column_names)
    #
    # data = re.findall(r"(USGS)\t(\d*)\t([\d-]*)\t([\d.]*)", text)
    #
    # column_names =['Agency', 'site_id', 'date', 'water_level_ft']
    # temp_usgs_dataframe = pd.DataFrame(data, columns=column_names)
    # print(temp_usgs_dataframe)
    #
    # usgs_dataframe = temp_usgs_dataframe.merge(name_df)
    # usgs_dataframe.set_index(['site_name', 'site_id'], inplace=True)
    # print(len(usgs_dataframe.index.levels[0]))
    # #print(usgs_dataframe)

    # return usgs_dataframe



# if __name__ == "__main__":
#     usgs_datagrab()