def usgs_datagrab():
    import urllib3
    import certifi
    import re
    import hydrofunctions as hf

    target_url = 'https://nwis.waterdata.usgs.gov/usa/nwis/uv/?referred_module=sw&site_tp_cd=LK&index_pmcode_72020=' \
                 '1&index_pmcode_72150=1&index_pmcode_72292=1&index_pmcode_62615=1&index_pmcode_62617=1&group_key=' \
                 'NONE&format=sitefile_output&sitefile_output_format=rdb&column_name=site_no&range_selection=' \
                 'date_range&begin_date=1950-10-01&end_date=2020-03-31&date_format=YYYY-MM-DD&rdb_compression=' \
                 'file&list_of_search_criteria=site_tp_cd%2Crealtime_parameter_selection'

    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
    req = http.request('GET', target_url)
    text = req.data.decode()
    retrieved_date = re.search(r"retrieved:.*$", text, re.M)
    print(retrieved_date.group())
    site_id_list = []
    for line in text.split('\n'):
        site_id = re.match(r"\d*[^A-Za-z]\b", line)
        if site_id:
            site_id_list.append(str(site_id.group()))
    print(site_id_list)

    start_date = '1950-10-01'
    for ids in site_id_list:
        test = hf.NWIS(ids, 'dv', start_date=start_date)
        try:
            test.get_data()
        except hf.exceptions.HydroNoDataError:
            print('No data for site ID: ' + str(ids))
        else:
            print(test.df().head())



usgs_datagrab()
