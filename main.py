from url_reader import url_reader
from data_grab import data_grab
from hierarchy import  hierarchy


def main():
    lake_database = url_reader()
    print(lake_database)
    hierarchy(lake_database)
    # data_grab(lake_database)


if __name__ == '__main__':
    main()
