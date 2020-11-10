from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
from ulmo.usgs.nwis.core import get_sites
from pandas.io.json._normalize import nested_to_record

begin_date = '1838-01-01'
now = datetime.now()
end_date = now.strftime('%Y-%m-%d')
# TODO Document what each parameter means or have a link to USGS website explaining
# param_list = ['72020','99020','30211','00062','99065',
#               '30207','00065','62600','72292','62615','62614','62616',
#               '62617','72214','72264','72275','72333','72335','72336',
#               '72199','72178']
#
# lake_list = 'https://waterdata.usgs.gov/nwis/site/?format=rdb&' \
#             'siteType=LK&variable=00062,00065,' \
#                      '30211,62600,62614,62615,62616,62617,62618,72020,' \
#                      '72292,72293,72333,99020,72178,72199,99065,30207,' \
#                      '72214,72264,72275,72335,72336&startDt=1838-01-01&endDt=2020-06-16'
            #'list_of_search_criteria=site_tp_cd%2Crealtime_parameter_selection'

def get_usgs_sites():
    """
    this function retrives every USGS site that has data from a set of pre-defined elevation parameters:

    :return: list of sites with water level data
    """
    #todo create dictionary of parameter id and names
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
                'group_key=NONE&format=sitefile_output&sitefile_output_format=rdb&state_cd=al&state_cd=ak&state_cd=aq' \
                '&state_cd=az&state_cd=ar&state_cd=bc&state_cd=ca&state_cd=eq&state_cd=co&state_cd=ct&state_cd=de' \
                '&state_cd=dc&state_cd=fl&state_cd=ga&state_cd=gu&state_cd=hi&state_cd=id&state_cd=il&state_cd=in' \
                '&state_cd=ia&state_cd=jq&' \
                'state_cd=ks&state_cd=ky&state_cd=la&state_cd=me&state_cd=md&state_cd=ma&state_cd=mi&state_cd=mq' \
                '&state_cd=mn&state_cd=ms&state_cd=mo&state_cd=mt&state_cd=ne&' \
                'state_cd=nv&state_cd=nh&state_cd=nj&state_cd=nm&state_cd=ny&state_cd=nc&state_cd=nd&state_cd=mp' \
                '&state_cd=oh&state_cd=ok&' \
                'state_cd=or&state_cd=pa&state_cd=pr&state_cd=ri&state_cd=yq&state_cd=sc&state_cd=sd&state_cd=sq' \
                '&state_cd=tn&state_cd=tx' \
                '&state_cd=tq&state_cd=bq&state_cd=iq&state_cd=ut&state_cd=vt&state_cd=vi&state_cd=va&state_cd=wq' \
                '&state_cd=wa&state_cd=wv&state_cd=wi&state_cd=wy&' \
                'site_tp_cd=LK&' \
                'column_name=site_no&column_name=station_nm&range_selection=date_range&' \
                'begin_date={}&end_date={}&date_format=YYYY-MM-DD&rdb_compression=file&' \
                'list_of_search_criteria=site_tp_cd%2Crealtime_parameter_selection'.format(begin_date, end_date)
    df = pd.read_csv(lake_list, sep="\t", comment="#", header=[0], low_memory=False).drop([0], axis=0)
    print("{} sites indexed".format(len(df["site_no"])))
    return df["site_no"].values


def update_usgs_meta():
    """
    Updates the meta table of valid sites and associated info
    :return: Pandas Dataframe of sites and associated information
    """
    sites = get_usgs_sites()
    print("{} sites indexed".format(len(sites)))
    big_df = []
    for count, site in enumerate(sites, 1):
        lake_m_list = "https://waterservices.usgs.gov/nwis/site/?format=rdb&sites={}&" \
                      "siteOutput=expanded&siteType=LK&siteStatus=all&hasDataTypeCd=iv,dv".format(site)
        df2 = pd.read_csv(lake_m_list, sep="\t", comment="#", header=[0], low_memory=False).drop([0], axis=0)
        big_df.append(df2)
        print("{}/{} sites indexed".format(count, sites))
    return pd.concat(big_df)


# def update_usgs_database(site_list):
#     """
#     Accesses the USGS NWIS website and scrapes the water surface level above NAVD 88 for available Lakes
#     :return: pandas multindex dataframe of lake name, USGS site ID, date of data acquisition, and associated water level
#     """
#     df = pd.DataFrame(columns=['lake_name', 'date', 'water_level'])
#     if isinstance(site_list, list) is True:
#         for i in site_list:
#             print('Processing site {}'.format(i))
#             target_url = 'http://waterservices.usgs.gov/nwis/dv/?sites={}&siteType=LK&startDT={}&endDT={}' \
#                          '&format=waterml&variable=00062,' \
#                          '30211,62600,62614,62615,62616,62617,62618,72020,' \
#                          '72292,72293,72333,99020,72178,72199'.format(i, begin_date, end_date).replace("%2C", ",")
#             req = requests.get(target_url)
#             soup = BeautifulSoup(req.content, "lxml-xml")
#             site_name = soup.find("siteName").get_text()
#             # site_code = soup.find("siteCode").get_text()
#             # crs = soup.find("geogLocation").attrs['srs']
#             # lat = soup.find("geogLocation").find("latitude").get_text()
#             # long = soup.find("geogLocation").find("longitude").get_text()
#             for elem in soup.find_all("value"):
#                 value = (elem.get_text())
#                 dateTime = elem.attrs['dateTime']
#                 df = df.append({"lake_name": site_name, "date": dateTime, "water_level": value}, ignore_index=True)
#             print('site {} Complete'.format(i))
#
#     elif isinstance(site_list, str) is True:
#         target_url = 'http://waterservices.usgs.gov/nwis/dv/?sites={}&siteType=LK&startDT={}&endDT={}' \
#                      '&format=waterml&variable=00062,' \
#                      '30211,62600,62614,62615,62616,62617,62618,72020,' \
#                      '72292,72293,72333,99020,72178,72199'.format(site_list, begin_date, end_date).replace("%2C", ",")
#         print("url", target_url)
#         req = requests.get(target_url)
#         soup = BeautifulSoup(req.content, "lxml-xml")
#         # df = pd.DataFrame(columns=['Name', 'Date', 'Water Level [m]'])
#         site_name = soup.find("siteName").get_text()
#         site_code = soup.find("siteCode").get_text()
#         crs = soup.find("geogLocation").attrs['srs']
#         lat = soup.find("geogLocation").find("latitude").get_text()
#         long = soup.find("geogLocation").find("longitude").get_text()
#         for elem in soup.find_all("value"):
#             value = (elem.get_text())
#             dateTime = elem.attrs['dateTime']
#             df = df.append({"lake_name": site_name, "date": dateTime, "water_level": value}, ignore_index=True)
#
#     else:
#         print("site_list must be a single str or list of str")
#     return df


def dict_2_dataframe(d):
    r = nested_to_record(d.values(), sep="_")
    return r[0]


if __name__ == "__main__":
    s = get_usgs_sites()
    large_df = update_usgs_meta(s)
    # from db_create import update_sql
    # update_sql(large_df, 'USGS_MTADTA')