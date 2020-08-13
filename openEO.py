import openeo
from openeo.internal.graph_building import PGNode
import json
import logging
from Bounds import bounds
import matplotlib.pyplot as plt
logging.basicConfig(level=logging.INFO)

# Define constants

# Connection
GEE_DRIVER_URL = "https://earthengine.openeo.org/v1.0"
OUTPUT_FILE = "/tmp/openeo_gee_output.png"
OUTFORMAT = "png"

# Auth
USER = "group1"
PASSWORD = "test123"

# Data
PRODUCT_ID = "LANDSAT/LC08/C01/T1_RT"

DATE_START = "2017-01-01T00:00:00Z"
DATE_END = "2017-01-31T23:59:59Z"

kml = '/Users/jakegearon/PycharmProjects/ESIP/LAke MEad.kml'

bounds = bounds(kml)

# Processes
MNDWI_GREEN = "B3"
MNDWI_MIR = "B6"

STRECH_COLORS_MIN = -1
STRECH_COLORS_MAX = 1



# Connect with GEE backend and authenticate with basic authentication
con = openeo.connect(GEE_DRIVER_URL)
con.authenticate_basic(USER, PASSWORD)

# Get available processes from the back end.
processes = con.list_processes()

# Retrieve the list of available collections
collections = con.list_collections()

print(list(collections)[:2])
result = (
    con
        .load_collection("LANDSAT/LC08/C01/T1_RT",
                         temporal_extent=["2020-01-01", "2020-03-10"],
                         spatial_extent=dict(zip(["west", "south", "east", "north"], bounds)),
                         bands=["B3", "B6"])
        .ndvi()
        .execute()
)



# Get detailed information about a collection
process = con.describe_collection('LANDSAT/LC08/C01/T1_RT')


# Select collection product to get a datacube object
datacube = con.load_collection("LANDSAT/LC08/C01/T1_RT",
                               spatial_extent={'west':minx,'east':maxx,'south':miny,
                                               'north':maxy,'crs':4326},
                               temporal_extent=["2016-01-01","2016-03-10"],
                               bands=['B3','B6'])

# Defining complex reducer
green = PGNode("array_element", arguments={"data": {"from_parameter": "data"}, "label": "B3"})
mir = PGNode("array_element", arguments={"data": {"from_parameter": "data"}, "label": "B8]6"})
mndwi = PGNode("normalized_difference", arguments={"x": {"from_node": mir}, "y": {"from_node": green}})

datacube = datacube.reduce_dimension(dimension="bands", reducer=mndwi)
datacube = datacube.min_time()

lin_scale = PGNode("linear_scale_range", arguments={"x": {"from_parameter": "x"}, "inputMin": -1, "inputMax": 1, "outputMax": 255})
datacube = datacube.apply(lin_scale)
datacube = datacube.save_result(format="PNG")

# Sending the job to the backend
job = datacube.send_job()
job.start_job()

# Describe Job
job.describe_job()

# B3 = datacube.band('B3')
# B6 = datacube.band('B6')
#
# res = datacube.process("normalized_difference", data=datacube, x=B3, y=B6)
#
# MNDWI = ((B3 - B6) / (B3 + B6))
#
# B3.download("out3.jpg",format="JPEG")
#
# MNDWI.download("out.jpg",format="JPEG")
#
# # Applying some operations on the data
# #
# # Defining NDVI reducer
# green = PGNode("array_element", arguments={"data": {"from_parameter": "data"}, "label": "B3"})
# mir = PGNode("array_element", arguments={"data": {"from_parameter": "data"}, "label": "B6"})
# mndwi = PGNode("normalized_difference", arguments={"x": {"from_node": mir}, "y": {"from_node": green}})
#
# # datacube = datacube.reduce(dimension="bands", reducer=mndwi)
# #
# # # take minimum time to reduce by time
# # datacube = datacube.min_time()
#
# # Linear scale necessary for GEE png export
# lin_scale = PGNode("linear_scale_range", arguments={"x": {"from_parameter": "x"}, "inputMin": -1, "inputMax": 1, "outputMax": 255})
# datacube = datacube.apply(lin_scale)
#
# # Save result as PNG
# datacube = datacube.save_result(format="PNG")
#
# plt.plot(datacube.graph)
#
# # Sending the job to the backend
# job = datacube.send_job()
# job.start_job()
#
# # Describe Job
# job.describe_job()
#
# job.download_result(OUTPUT_FILE)
#
# # Showing the result
# from IPython.display import Image
# result = Image(filename=OUTPUT_FILE)
#
# result