from grealm_data_multindex import grealm_data_multindex
from usgs_datagrab import usgs_datagrab
from HydroWeb_grabber import hydro_grab


def main():
    grealm_lake_database = grealm_data_multindex()
    usgs_lake_database = usgs_datagrab()
    hydroweb_database = hydro_grab()
    print(grealm_lake_database)
    print(usgs_lake_database)
    print(hydroweb_database)


if __name__ == '__main__':
    main()
