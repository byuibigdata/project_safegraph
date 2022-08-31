# https://towardsdatascience.com/cleaning-and-extracting-json-from-pandas-dataframes-f0c15f93cb38
# https://packaging.python.org/tutorials/packaging-projects/
import pandas as pd
import json
import re

def jsonloads(x):
    if pd.isna(x):
        return None
    if x == '':
        return None
    else:
        return json.loads(x)

def createlist(x):
    try:
        return x.str.strip('][').str.split(',')
    except:
        return None

def rangenumbers(x):
    if x.size == 1:
        return 0
    else:
        return range(1, x.size + 1)


# changed to only output long format and match the list output
def expand_json(var, dat):

    rowid = dat.placekey
    start_date = dat.date_range_start
    end_date = dat.date_range_end

    parsedat = dat[var]
    loadsdat = parsedat.apply(jsonloads)

    temp_list = list()
    for i in range(rowid.size):
        if loadsdat[i] != None:
            tempi = (pd.json_normalize(loadsdat[i])
                .melt()
                .rename(columns = {'variable':var})
                .assign(
                    placekey = rowid[i],
                    startDate =  start_date[i],
                    endDate = end_date[i]
                )
                .filter(['placekey', 'startDate',
                    'endDate', var, 'value'])
            )
            temp_list.append(tempi)
    
    out = pd.concat(temp_list)

    return out    
    

def expand_list(var, dat):

    dat_expand = (dat
        .assign(lvar = createlist(dat[var]))
        .filter(["placekey", "date_range_start",
            "date_range_end","lvar"])
        .explode("lvar")
        .rename(columns={"lvar":var})
        .dropna(subset = ['date_range_start', 'date_range_end'])
        .query("{0} != ''".format(var))
        .reset_index(drop=True)
    )

    dat_label = (dat_expand
        .groupby(
            ['placekey', 'date_range_start', 'date_range_end'],
            sort = False)
        .transform(lambda x: rangenumbers(x))
        .reset_index(drop=True)
    )
    
    if var.find("hour") !=-1:
        orderName = 'hour'
    elif var.find("day") !=-1:
        orderName = 'day'
    else :
        orderName = 'sequence'
    
    #dat_label.columns = ['sequence']
    dat_label.rename(columns = {var:orderName}, inplace=True)
    if dat_label.shape[0] != dat_expand.shape[0]:
        print("Concat not same size")
        return None
    out = pd.concat([dat_expand, dat_label], axis=1).reset_index(drop=True)
    out[var] = out[var].astype(float)

    out = (out.rename(columns = {
        'date_range_start':'startDate', 'date_range_end':'endDate'})
        .filter(
            ['placekey', 'startDate', 'endDate', orderName, var],axis=1)
    )

    return out
