from sklearn.base import TransformerMixin
import pandas as pd
import numpy as np
# from dateparser import parse
from dateutil.parser import parse
from joblib import Parallel, delayed, Memory


# import ipyparallel as ipp
# ipp.register_joblib_backend()

def isfloat(x_):
    try:
        float(x_)
        return True
    except:
        return False


def isdate(string):
    try:
        parse(string)
        return True
    except:
        return False


def pTransformFloat(X, col):
    X[col + '_isfloat'] = X[col].apply(isfloat)
    return X


def pTransformDate(X, col):
    X = pd.to_datetime(X[col], errors='coerce', exact=False, infer_datetime_format=True).notnull().astype('int')
    return X


class isfloatTransform(TransformerMixin):
    def __init__(self, column_name):
        self.column_name = column_name

    def transform(self, X, **transform_params):
        slices = Parallel(n_jobs=-1, backend='multiprocessing')(
            delayed(pTransformFloat)(df, self.column_name)
            for (filename, page), df in X.groupby(['index', 'PageNumber']))
        X = pd.concat(slices, axis=0)

        print(len(X), len(X.columns), 'isfloat in')
        return X[self.column_name + '_isfloat']

    def fit(self, X, y=None, **fit_params):
        return self


class isdateTransform(TransformerMixin):
    def __init__(self, column_name):
        self.column_name = column_name

    def transform(self, X, **transform_params):
        print(len(X), len(X.columns), 'isdate in')
        X[self.column_name + '_isdate'] = pd.concat(Parallel(n_jobs=-1, backend='multiprocessing')(
            delayed(pTransformDate)(page, self.column_name) for name, page in X.groupby(['index', 'PageNumber'])))
        return X[self.column_name + '_isdate']

    def fit(self, X, y=None, **fit_params):
        return self


class istextTransform(TransformerMixin):
    def __init__(self, column_name):
        self.column_name = column_name

    def transform(self, X, **transform_params):
        print(len(X), len(X.columns), 'istext in')
        cols = [self.column_name + '_istext']
        if (self.column_name + '_isdate') not in X.columns:
            X[self.column_name + '_isdate'] = isdateTransform(self.column_name).transform(X)
            cols.append(self.column_name + '_isdate')
        if (self.column_name + '_isfloat') not in X.columns:
            X[self.column_name + '_isfloat'] = isfloatTransform(self.column_name).transform(X)
            cols.append(self.column_name + '_isfloat')
        X[self.column_name + '_istext'] = (
        ~((X[self.column_name + '_isfloat']).astype('bool'))).astype('int')
        return X

    def fit(self, X, y=None, **fit_params):
        return self


'''
class isdateTransform(TransformerMixin):
    def __init__(self, column_name):
        self.column_name = column_name
    def transform(self, X, **transform_params):
        print(len(X),len(X.columns),'isdate in')
        slices = Parallel(n_jobs=-1,backend='multiprocessing')(
            delayed(pTransformDate)(df,self.column_name)
            for (filename,page),df in X.groupby(['sourceS3Key','pageNumber']))
        X = pd.concat(slices,axis=0)

        #X[col+'_isdate'] = X[col].apply(isdate)
        return X[self.column_name+'_isdate']
    def fit(self, X, y=None, **fit_params):
        return self
'''