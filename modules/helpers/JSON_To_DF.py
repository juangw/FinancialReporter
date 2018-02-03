import json
import pandas as pd
import numpy as np
import copy


# This is a recursive function that adds all the keys from the child object to the parent object
# And then iterating further down through arrays and returning and array
def iterateObject(base_obj, child_obj):
    # Create a copy so we don't edit the original
    myobj = copy.copy(base_obj)

    toret = []

    # Keep track of the keys we will need to iterate down through
    list_keys = []
    for key in child_obj:
        # If it is not a list, or the children of that list are not dictionaries
        # Add it to the base object
        if (not isinstance(child_obj[key], list) or (len(child_obj[key]) > 0 and not isinstance(child_obj[key][0], dict))):
            myobj[key] = child_obj[key]
        else:
            # Otherwise, append this key for later
            list_keys.append(key)

    # If we have keys to iterate through now we need to recusrively go down the object.
    if (len(list_keys) > 0):
        for key in list_keys:
            for arr_obj in child_obj[key]:
                toret.extend(iterateObject(myobj, arr_obj))
    else:
        # If there were no keys to iterate through it is safe to return just the object we created
        toret.append(myobj)

    return toret


def JSONToDataframe(json_obj):
    # Iterate with a blank object, called on the object we get.
    arr = iterateObject({}, json_obj)
    print(arr)
    for row in arr:
        for key in row:
            # If the array only has one element, simply propagate it up out
            # Pandas seems to do this for you on smaller objects, but on bigger ones
            # It does not?
            # Otherwise these would show up as ['string'] in the field.
            # TODO Add support for exploding out multiple children elements into distinct arrays (taking into account multiples for any present arrays, would quickly get out of hand.)
            if (isinstance(row[key], list) and len(row[key]) == 1):
                row[key] = row[key][0]
    df = pd.DataFrame(arr)
    return df


def ParallelDownloadJSON_S3FS(file, threshold=0):
    if (np.random.random() >= threshold):
        try:
            js_str = file.read()
            j_obj = json.loads(js_str)
            df = JSONToDataframe(j_obj)
            return df
        except:
            pass
    else:
        pass