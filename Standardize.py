import pandas as pd
import re
from difflib import get_close_matches
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

# grlm_names = [i.lower() for i in df_grealm_f.index.levels[0]]
# hydro_names = [i.lower().split('\blake\b')[1].strip(' ') for i in (df_hydroweb_f.index.levels[0])]
# usgs_names = [re.split('\sat|\snear\s|\sabove\s|\seast\sof\s|\snorth\sof\s|\ssouth\sof\s|\swest\sof\s|\sn\sof\s|\ss\sof\s|\sw\sof\s|\se\sof\s|\snr\s|\sabv\s|\sab\s|\son\s|\s@\s',
#                        i.lower())[0] for i in df_usgs_f.index.levels[0]]
#
# replacements = {'lake':'',
#                 'lk': '',
#                 'res': '',
#                 'reservoir':'',
#                 'pond':'',
#                 'lago':''}
# def replace(match):
#     return replacements[match.group(0)]
#
# stripped_usgs = []
# for i in usgs_names:
#     strp = re.sub('|'.join(r'\b%s\b' % re.escape(s) for s in replacements), replace, i)
#     stripped_usgs.append(strp.strip())
#
# #stripped_usgs = [i.replace(' lake\b', '').replace(' lk\b', '').replace(' res\b', '').replace(' reservoir\b','').replace(' pond ', '').replace(' lago ', '') for i in usgs_names]
#
# # srch = re.compile(r'(.*) (at|near|above|east of|north of|south of|west of|n of|s of|w of|e of|nr|abv)')
# # .split('near')[1].split('above')[1].split('east of')[1].split('north of').split('south of').split('west of').split('n of').split('s of').split('w of').split('e of').split('nr').split('abv')
# # result = [f.group(1) for f in (srch.match(line) for line in usgs_names) if f]
# # result2 = [f.group(1) for f in (srch.match(line) for line in result) if f]
# cnt = 0
# for i in df_usgs_f.index.levels[0]:
#     #match = get_close_matches(i, hydro_names)
#     matching = [s for s in hydro_names if i == s]
#     m = re.split('\sat|\snear\s|\sabove\s|\seast\sof\s|\snorth\sof\s|\ssouth\sof\s|\swest\sof\s|\sn\sof\s|\ss\sof\s|\sw\sof\s|\se\sof\s|\snr\s|\sabv\s', i.lower())
#     #df_usgs_f.index.levels[0]]
#     print(i, m)
#     cnt += 1
# print(cnt)


