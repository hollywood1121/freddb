
## new API puller and database stuffer
import pandas as pd
import pyodbc

from full_fred.fred import Fred


# function dropandtimechange
# purpose: sterilizes, drop uneeded rows, and cleans data for import
# for a MS db that already has predetermined columns. this is a great way
# to keep things contained and keep repetitive tasks together.
# args: takes a dictionary of all the economic data.
# returns: a pandas dataframe of merged data on date.
def dropandtimechange(masterlist):
    index = 0
    masterdf = pd.DataFrame()
    for i in masterlist:
        # this represents the cleaning portion / data prep of ETL (transforming)
        # make sure to convert dates, checking for null values to respond appropriately
        # make sure to convert value to float

        masterlist[i] = masterlist[i].drop(['realtime_start', 'realtime_end'], axis=1)
        masterlist[i]['date'] = pd.to_datetime(masterlist[i]['date'])
        masterlist[i]['value'] = masterlist[i]['value'].astype(float)
        masterlist[i] = masterlist[i].rename(columns={'value': i})
        # check for null values via info
        masterlist[i].info()

        # if null values, drop row.



        # check for duplicate values
        masterlist[i].nunique()

        # if nunique values, drop
        # masterlist[i].drop_duplicates()

        # if number one, set as master, else merge
        if index == 0:
            masterdf = masterlist[i]
        else:
            masterdf = masterdf.merge(masterlist[i])

        index += 1

    ######################### old code #########################
    # pull a subset of data, keep this for reference. this is saying only keep the 2005-01-01 and up
    # masterlist[i] = masterlist[i][~(masterlist[i]['date'] < '2005-01-01')]
    # we don't need to reset it anymore because we queried it right.
    # masterlist[i] = masterlist[i].reset_index()
    #########################  end   ###########################



    # we need to merge all based on date using group(), but the column name needs to be the key used.
    # return the list then we hit the DB!
    return masterdf


# function sqlpush
# purpose: calls MS db and opens the connection to insert rows.
# args: pandas dataframe
# returns: nothing.
def sqlpush(pddf):
    # fire up sql connection
    f = open("sqlconn.txt")
    sql_split = f.read().split(",")
    server = sql_split[0]
    database = sql_split[1]
    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s;DATABASE=%s;Trusted_Connection=yes;' % (server, database))
    cursor = cnxn.cursor()
    for index, row in pddf.iterrows():
        cursor.execute("INSERT INTO dbo.[Fred.Econ.Data] (Date,mspus,eciwag,fedminnfrwg,frbkclmcim) values(?,?,?,?,?)",
                       row.date, row.mspus, row.eciwag, row.fedminnfrwg, row.frbkclmcim)
    cnxn.commit()
    cursor.close()



fred = Fred('apikey.txt')

# define variables to take from
mspus = fred.get_series_df(series_id="mspus", observation_start="2005-01-01", observation_end="2021-01-01", frequency="q")
eciwag = fred.get_series_df(series_id="eciwag", observation_start="2005-01-01", observation_end="2021-01-01", frequency="q")
fedminnfrwg = fred.get_series_df(series_id="fedminnfrwg", observation_start="2005-01-01", observation_end="2021-01-01", frequency="q")
frbkclmcim = fred.get_series_df(series_id="frbkclmcim", observation_start="2005-01-01", observation_end="2021-01-01", frequency="q")

dfs1 = {'mspus': mspus, 'eciwag': eciwag, 'fedminnfrwg': fedminnfrwg, 'frbkclmcim':frbkclmcim }

# this is executing everything
dfs2 = dropandtimechange(dfs1)

# this pushes into db
sqlpush(dfs2)