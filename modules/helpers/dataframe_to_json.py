import json
import pandas as pd
import copy



#Put's dataframe into  python dictionary, then dumps into JSON object
def DataframeToJSON(df):
    d = [
        dict([
                (colname, row[i])
                for i,colname in enumerate(df.columns)
        ])

        for row in df.values
    ]
    return json.dumps(d)
