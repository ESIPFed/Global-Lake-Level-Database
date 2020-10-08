import pandas as pd
from sqlalchemy import create_engine
import pymysql
from io import BytesIO
from zipfile import ZipFile
import urllib.request
import pandas as pd
# %% Section: MetaInfo
__author__ = 'John Franey'
__credits__ = ['John Franey', 'Jake Gearon']

__version__ = '1.0.0'
__maintainer__ = 'John Franey'
__email__ = 'franeyjohn96@gmail.com'
__status__ = 'Development'
# %%
sql_engine = create_engine('mysql+pymysql://***REMOVED***:***REMOVED***'
                           '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()
connection = pymysql.connect(host='lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com',
                             user='***REMOVED***',
                             password='***REMOVED***',
                             db='laketest',
                             connect_timeout=100000,
                             )
cursor = connection.cursor()

hydroweb_sql = u"SELECT `id_No`," \
               u"`lake_name`," \
               u"metadata->'$.identifier' AS `hydroweb_ID`" \
               u" FROM reference_ID WHERE `source` = 'hydroweb'"

# Read in Grealm unique lake ID and observation dates from reference Table
# Clean up the grealm_lakes_info dataframe
hydroweb_lakes_info = pd.read_sql(hydroweb_sql, con=sql_engine)
hydroweb_lakes_info['hydroweb_ID'] = hydroweb_lakes_info['hydroweb_ID'].str.strip('"')
# %% Section: Create df of all available hydroweb data
ls_df = []
url = "http://hydroweb.theia-land.fr/hydroweb/authdownload?products=lakes&user=jake.gearon@gmail.com&pwd=vHs98NdXe9BxWNyok*Pp&format=txt"
target_url = urllib.request.urlopen(url)
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
                                'Area (km2)', 'Volume with respect to volume of first date (km3)', 'Flag']
                         )
        df['lake_name'] = name.capitalize()
        # unique_id = hydroweb_lakes_info.loc[hydroweb_lakes_info['lake_name'] == name, 'id_No']
        df['id_No'] = unique_id
        ls_df.append(df)
raw_lake_level_df = pd.concat(ls_df, ignore_index=True, copy=False)

# %% Section:

