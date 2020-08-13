from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
from ulmo.usgs.nwis.core import get_sites
import string
from pandas.io.json._normalize import nested_to_record

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
    """
    this function retrives every USGS site that has data from a set of pre-defined elevation parameters:

    :return: list of sites with water level data
    """
    #todo create dictionary of parameter id and names
    lake_document = requests.get(lake_list)
    lakesoup = BeautifulSoup(lake_document.content, "lxml-xml")
    site_no_list = [i.text for i in lakesoup.find_all("site_no")]
    print("{} sites indexed".format(len(site_no_list)))
    return site_no_list


def update_usgs_meta(sites):
    """
    Updates the meta table of valid sites and associated info
    :param sites: list or str of usable USGS NWIS Lakes with site information
    :return: Pandas Dataframe of sites and associated information
    """
    #todo make keys of usgs site info to pass to dataframe
    big_df = pd.DataFrame(columns=['code', 'name', 'network', 'agency', 'county', 'huc', 'site_type',
                                   'state_code', 'location_latitude', 'location_longitude', 'location_srs',
                                   'timezone_info_uses_dst', 'timezone_info_dst_tz_abbreviation',
                                   'timezone_info_dst_tz_offset', 'timezone_info_default_tz_abbreviation',
                                   'timezone_info_default_tz_offset'])
    if isinstance(sites, str) is True:
        site_data = get_sites(sites=sites,service='dv', site_type='LK')
        d = dict_2_dataframe(site_data)
        cols = d.keys()
        # todo append iteratively to sql table??
        df = pd.DataFrame(d, index=[0])
        big_df = big_df.append(df)
        return big_df
        # df.to_sql()
    elif isinstance(sites, list) is True:
        for i in sites:
            site_data = get_sites(sites=i, service='dv', site_type='LK')
            d = dict_2_dataframe(site_data)
            cols = d.keys()
            #todo append iteratively to sql table??
            df = pd.DataFrame(d, index=[0])
            big_df = big_df.append(df)
        return big_df
            #df.to_sql()


def update_usgs_database(site_list):
    """
    Accesses the USGS NWIS website and scrapes the water surface level above NAVD 88 for available Lakes
    :return: pandas multindex dataframe of lake name, USGS site ID, date of data acquisition, and associated water level
    """
    df = pd.DataFrame(columns=['Name', 'Date', 'Water Level [m]'])
    if isinstance(site_list, list) is True:
        for i in site_list:
            target_url = 'http://waterservices.usgs.gov/nwis/dv/?sites={}&siteType=LK&startDT={}&endDT={}' \
                         '&format=waterml&variable=00062,' \
                         '30211,62600,62614,62615,62616,62617,62618,72020,' \
                         '72292,72293,72333,99020,72178,72199'.format(i, begin_date, end_date).replace("%2C", ",")
            req = requests.get(target_url)
            soup = BeautifulSoup(req.content, "lxml-xml")
            site_name = soup.find("siteName").get_text()
            site_code = soup.find("siteCode").get_text()
            crs = soup.find("geogLocation").attrs['srs']
            lat = soup.find("geogLocation").find("latitude").get_text()
            long = soup.find("geogLocation").find("longitude").get_text()
            for elem in soup.find_all("value"):
                value = (elem.get_text())
                dateTime = elem.attrs['dateTime']
                df = df.append({"Name": site_name, "Date": dateTime, "Water Level [m]": value}, ignore_index=True)



    elif isinstance(site_list, str) is True:
        target_url = 'http://waterservices.usgs.gov/nwis/dv/?sites={}&siteType=LK&startDT={}&endDT={}' \
                     '&format=waterml&variable=00062,' \
                     '30211,62600,62614,62615,62616,62617,62618,72020,' \
                     '72292,72293,72333,99020,72178,72199'.format(site_list, begin_date, end_date).replace("%2C", ",")
        print("url", target_url)
        req = requests.get(target_url)
        soup = BeautifulSoup(req.content, "lxml-xml")
        #df = pd.DataFrame(columns=['Name', 'Date', 'Water Level [m]'])
        site_name = soup.find("siteName").get_text()
        site_code = soup.find("siteCode").get_text()
        crs = soup.find("geogLocation").attrs['srs']
        lat = soup.find("geogLocation").find("latitude").get_text()
        long = soup.find("geogLocation").find("longitude").get_text()
        for elem in soup.find_all("value"):
            value = (elem.get_text())
            dateTime = elem.attrs['dateTime']
            df = df.append({"Name": site_name, "Date": dateTime, "Water Level [m]": value}, ignore_index=True)

    else:
        print("site_list must be a single str or list of str")
    return df


def dict_2_dataframe(d):
    r = nested_to_record(d.values(), sep="_")
    return r[0]


if __name__ == "__main__":
    s = get_usgs_sites()
    large_df = update_usgs_meta(s)
    from db_create import update_sql
    update_sql(large_df, 'USGS_MTADTA')