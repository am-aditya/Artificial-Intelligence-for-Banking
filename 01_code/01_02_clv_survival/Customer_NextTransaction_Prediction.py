# This code is used to score a deep learning model to predict the transaction amount and days of a customer.
# The model has been built on a lot of data and what you see here is just a sample of that data.

# Model : Deep Neural Network Regression
# Created on : 27-Aug-2018

# Importing all required libraries
import os
import sys
import time
import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime
import json
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
from keras.models import Sequential
from keras.layers import Dense
from keras.wrappers.scikit_learn import KerasRegressor
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import MinMaxScaler
from keras.models import model_from_json

sys.__stdout__ = sys.stdout
  
# Function for the CLV clustering model training
def clv_predict_amt(customer_id):

    all_data = pd.read_csv('..\\..\\99_sample_data\\customerpreddata.csv')
    all_data = all_data.fillna(0)

    # Pull data for one customer (Temporary)
    # all_data = all_data[all_data['AccountNo']==customer_id]
    
    print('Cleaning and Transforming Data.')
    # Convert all data to categorical
    all_data = all_data.drop('lag_Datediff',1)
    all_data['Gender'] = pd.Categorical(all_data.Gender).codes
    all_data['ContactAvailability'] = pd.Categorical(all_data.ContactAvailability).codes
    all_data['HouseType'] = pd.Categorical(all_data.HouseType).codes
    all_data['HomeCountry'] = pd.Categorical(all_data.HomeCountry).codes
    all_data['TransactionCountry'] = pd.Categorical(all_data.TransactionCountry).codes
    all_data['TransactionCurrencyCode'] = pd.Categorical(all_data.TransactionCurrencyCode).codes
    all_data['Week_Segment'] = pd.Categorical(all_data.Week_Segment).codes  
    all_data['Product'] = pd.Categorical(all_data.Product).codes  
    ##################################################################

    all_unique_acctno = all_data.AccountNo.unique()
    temp_all_data = all_data
    scalarX, scalarY = MinMaxScaler(), MinMaxScaler()
    
    print('Attempting to read the built Keras Regression Deep Learning Model...')
    
    ############################# load json and create model
    json_file = open('..\\..\\02_models\\clv_amt_dl_model.json', 'r')
    loaded_model_json = json_file.read()
    json_file.close()
    loaded_model = model_from_json(loaded_model_json)
    
    # load weights into new model
    loaded_model.load_weights('..\\..\\02_models\\clv_amt_dl_model.h5')
    print("Loaded model from disk")
     
    # evaluate loaded model on test data
    loaded_model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
    #########################

    for curr_act in all_unique_acctno:

        all_data = temp_all_data[temp_all_data['AccountNo'] == curr_act]
        
        # split into input (X) and output (Y) variables
        X = all_data.iloc[0:len(all_data)-1,1:15] # dont consider account no
        Y = all_data.iloc[0:len(all_data)-1,16] # lag transaction amount column
        #print(Y.head())

        loaded_model.fit(X, Y, epochs=100, verbose=0)

        # Sample Testing
        # split into input (Xnew) and output (Yold) variables
        
        Xnew = all_data.tail(1) # dont consider account no
        Xnew = Xnew.ix[:,1:15]
        ynew = loaded_model.predict(Xnew)
        
        print("Predicted Next Transaction (Amount)  = {}".format(str(ynew)))

        break # Remove this to do it for all customers. Breaking after 1 customer since this is a sample    
    

def clv_predict_days(customer_id):

    all_data = pd.read_csv('..\\..\\99_sample_data\\customerpreddata.csv')
    all_data = all_data.fillna(0)    

    print('Cleaning and Transforming Data.')
    # Convert all data to categorical
    all_data = all_data.drop('lag_Txamt',1)
    all_data['Gender'] = pd.Categorical(all_data.Gender).codes
    all_data['ContactAvailability'] = pd.Categorical(all_data.ContactAvailability).codes
    all_data['HouseType'] = pd.Categorical(all_data.HouseType).codes
    all_data['HomeCountry'] = pd.Categorical(all_data.HomeCountry).codes
    all_data['TransactionCountry'] = pd.Categorical(all_data.TransactionCountry).codes
    all_data['TransactionCurrencyCode'] = pd.Categorical(all_data.TransactionCurrencyCode).codes
    all_data['Week_Segment'] = pd.Categorical(all_data.Week_Segment).codes  
    all_data['Product'] = pd.Categorical(all_data.Product).codes  
    ##################################################################

    all_unique_acctno = all_data.AccountNo.unique()
    temp_all_data = all_data
    scalarX, scalarY = MinMaxScaler(), MinMaxScaler()

    print('Attempting to read the built Keras Regression Deep Learning Model...')
    
    ############################# load json and create model
    json_file = open('..\\..\\02_models\\clv_days_dl_model.json', 'r')
    loaded_model_json = json_file.read()
    json_file.close()
    loaded_model = model_from_json(loaded_model_json)
    
    # load weights into new model
    loaded_model.load_weights('..\\..\\02_models\\clv_days_dl_model.h5')
    print("Loaded model from disk")
     
    # evaluate loaded model on test data
    loaded_model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
    #########################
    
    for curr_act in all_unique_acctno:
        
        all_data = temp_all_data[temp_all_data['AccountNo'] == curr_act]

        # split into input (X) and output (Y) variables
        X = all_data.iloc[0:len(all_data)-1,1:15] # dont consider account no
        Y = all_data.iloc[0:len(all_data)-1,16] # lag transaction amount column

        loaded_model.fit(X, Y, epochs=10, verbose=0)
        
        # Sample Testing
        # split into input (Xnew) and output (Yold) variables
        Xnew = all_data.tail(1) # dont consider account no
        Xnew = Xnew.ix[:,1:15]
        ynew = loaded_model.predict(Xnew)
        print("Customer ID = {}".format(str(curr_act)))
        print("Predicted Next Transaction (Days) = {}".format(str(ynew)))

        break # Remove this to do it for all customers. Breaking after 1 customer since this is a sample    

    print('Scoring Process Complete.')
 

def main():

    # For testing only
    customer_id = 1100722 # For training only
    clv_predict_amt(customer_id)
    clv_predict_days(customer_id)
    
if __name__ == "__main__":
    main() 


    
    
