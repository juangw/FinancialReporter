from sklearn.base import TransformerMixin
import pandas as pd
import numpy as np
from joblib import Parallel, delayed, Memory
def isfloat(x_):
    try:
        float(x_)
        return True
    except:
        return False
class AlignmentTransform(TransformerMixin):
        #When the transformer is initiated
        #We pass it the list of columns to use
        #We pass it a dataframe   
        #We pass it the name of the column we want to output to              
        #Example being ['N_Bottom','Y','pageIndex','Filename']
        #Where the first two are the ones used to determine the actual alignment
        #And the second two are to determine if the records are comparable (they must equal)
        #Additionally we may choose to presort the data in the columns
        #Sort_columns are the columns, and sort_values are the ascending/descending values
        def __init__(self, columns,output_name,sort_columns = [],sort_values = []):
            self.columns = columns
            self.sort_columns = sort_columns
            self.sort_values = sort_values
            self.output_name = output_name

        def pTransform(self,X):
            if(len(self.sort_columns) > 0 and (len(self.sort_columns) == len(self.sort_values))):
                #for i in range(0, len(self.sort_columns)):
                    #X.sort_values(self.sort_columns[i], ascending = self.sort_values[i], inplace=True)
                X[self.sort_columns]=X[self.sort_columns].astype(float)
                X=X.sort_values(self.sort_columns,ascending = self.sort_values)
                #X.to_csv('checksort.csv')
            lineindex = 0
            #We do need to grab lineindex


            #Zero them out (and create the column if it doesn't exist)
            

            #We only grab the columns we are going to use
            line_data = X.loc[min(X.index)].filter(self.columns)
            #print(line_data)

            #This iterates through all the rows
            for i in X.index:
                temp = X.loc[i].filter(self.columns)
                #Checking to make sure they are correct by the filter columns
                if (temp[2] == line_data[2] and temp[3] == line_data[3]):
                    #Then basically checks to make sure they're in the same row
                    if (temp[1] <= line_data[0]):
                        X.loc[i,self.output_name]=line_data[4]
                    else:
                        #Increment the output column index otherwise.
                        line_data[4] = line_data[4] + 1
                        X.loc[i,self.output_name] = line_data[4]
                        line_data[0] = temp[0]
                else:
                    line_data = temp
            #print(len(X),'textalign')
            return X

        def transform(self, X, **transform_params):
            if(self.output_name not in self.columns):
                self.columns.append(self.output_name)
            X[self.output_name] = np.zeros(len(X))
            print(len(X),len(X.columns),'TextAlignment in')
            slices = Parallel(n_jobs=-1,backend='multiprocessing')(delayed(self.pTransform)(df) for (page), df in X.groupby(['PageNumber'],sort=False))
            X = pd.concat(slices,axis=0)
            return X.filter([self.output_name]).astype(int)

        def fit(self, X, y=None, **fit_params):
            return self