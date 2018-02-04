import pandas as pd
from sklearn.base import TransformerMixin


class CenterPositionTransform(TransformerMixin):
    def __init__(self, xNames=['X_Min', 'X_Max'],
                 yNames=['Y_Min', 'Y_Max'],
                 outputNames=['X_Center', 'Y_Center']):
        self.xNames = xNames
        self.yNames = yNames
        self.outputNames = outputNames
        if (len(xNames) != len(yNames)):
            raise Exception('Input arrays must all be same length')
        if (len(outputNames) != 2):
            raise Exception('Output array must be of length 2')

    def transform(self, X, **transform_params):
        print(len(X), len(X.columns), 'centerposition in')
        # If the word label equals the label, go ahead and make it a 1
        for col in [self.xNames + self.yNames]:
            X[col] = X[col].astype('float32')
        X[self.outputNames[0]] = (X[self.xNames].sum(axis=1)) / len(self.xNames)
        X[self.outputNames[1]] = (X[self.yNames].sum(axis=1)) / len(self.yNames)
        return X

    def fit(self, X, y=None):
        return self