import pandas as pd
from usgs_datagrab import usgs_datagrab
from grealm_data_multindex import grealm_data_multindex
from HydroWeb_grabber import hydro_grab

df_usgs = usgs_datagrab()
df_grealm = grealm_data_multindex()
df_hydroweb = hydro_grab()

df_grealm_filt = df_grealm.filter([2, 14])
df_grealm_filt = df_grealm_filt[df_grealm_filt[2] != '99999999']
df_grealm_filt = df_grealm_filt[df_grealm_filt[14] != 9999.99]
df_grealm_filt['Date'] = pd.to_datetime(df_grealm_filt[2], format='%Y%m%d')
df_grealm_f = df_grealm_filt.drop([2], axis=1)
df_grealm_f = df_grealm_f.rename(columns={14:"water_level"})
df_grealm_f = df_grealm_f[['Date', 'water_level']]

df_hydroweb_filt = df_hydroweb.filter(['Date | yyyy/mm/dd', 'Height above surface of ref (m)'])
df_hydroweb_filt['Date'] = pd.to_datetime(df_hydroweb_filt['Date | yyyy/mm/dd'], infer_datetime_format=True)
df_hydroweb_f = df_hydroweb_filt.drop(['Date | yyyy/mm/dd'], axis=1)
df_hydroweb_f = df_hydroweb_f.rename(columns={"Height above surface of ref (m)":"water_level"})
df_hydroweb_f = df_hydroweb_f[['Date', 'water_level']]

df_usgs_filt = df_usgs.filter(['site_id', 'date', 'water_level_ft'])
df_usgs_filt['Date'] = pd.to_datetime(df_usgs_filt['date'], infer_datetime_format=True)
df_usgs_f = df_usgs_filt.drop(['date'], axis=1)
df_usgs_f['water_level_ft'] = pd.to_numeric(df_usgs_f['water_level_ft'])
df_usgs_f['water_level'] = df_usgs_f['water_level_ft'] * .3048
df_usgs_f = df_usgs_f.drop(['water_level_ft'], axis=1)

grlm_names = [i.lower().strip('lake ') for i in (df_grealm_f.index.levels[0])]
usgs_names = [i.lower().strip('lake ') for i in (df_hydroweb_f.index.levels[0])]
hydro




