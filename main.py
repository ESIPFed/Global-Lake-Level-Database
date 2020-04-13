from grealm_data_multindex import grealm_data_multindex
from usgs_datagrab import usgs_datagrab


def main():
    grealm_lake_database = grealm_data_multindex()
    usgs_lake_database = usgs_datagrab()
    print(grealm_lake_database)
    print(usgs_lake_database)


if __name__ == '__main__':
    main()
