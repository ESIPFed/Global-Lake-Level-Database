# %% Section: MetaInfo
__author__ = ['John Franey', 'Jake Gearon']
__credits__ = ['John Franey', 'Jake Gearon', 'Earth Science Information Partners (ESIP)']
__version__ = '1.0.0'
__maintainer__ = 'John Franey'
__email__ = 'franeyjohn96@gmail.com'
__status__ = 'Development'
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '\u15E7', printEnd = "\r"):
    """
    [Code from Greenstick on StackOverflow](https://stackoverflow.com/a/34325723/13617277)
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    already_filled = '\u25e6'
    bar = already_filled * (filledLength-1) + fill + '\u2022' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()

def get_ref_table():
    import pandas as pd
    from sqlalchemy import create_engine

    sql_engine = create_engine('mysql+pymysql://***REMOVED***:***REMOVED***'
                               '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()

    df = pd.read_sql('select * from reference_ID', con=sql_engine)
    sql_engine.close()
    return df

def get_lake_table():
    import pandas as pd
    from sqlalchemy import create_engine

    sql_engine = create_engine('mysql+pymysql://***REMOVED***:***REMOVED***'
                               '@lake-test1.cevt7olsswvw.us-east-2.rds.amazonaws.com:3306/laketest').connect()

    df = pd.read_sql('select * from lake_water_level', con=sql_engine)
    sql_engine.close()
    return df
