# This code is used to score a machine learning model to enable to predict a CLV value for new customers
# Model : K Means Estimator using H2O Deep Learning Models
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
import h2o
from sklearn.preprocessing import StandardScaler,MinMaxScaler,RobustScaler,Normalizer
from h2o.estimators.deeplearning import H2OAutoEncoderEstimator
from h2o.estimators.kmeans import H2OKMeansEstimator


sys.__stdout__ = sys.stdout
  
# Function for the CLV clustering model training
def clv_clustering_scoring(new_customer_details):

    # Reading data from the sample csv file
    print("Processing Step 1 --> Reading in the sample data")
    all_data = pd.read_csv('..\\..\\99_sample_data\\custclv.csv')

    #h2o.shutdown()
    h2o.init(ip="127.0.0.1", https=True, insecure = True) # initializing h2o
    predictors = ['rfm_score','Gender','Age','HouseType','ContactAvailability','HomeCountry',
                  'CreditScore','CLV','MonthlyValue','ActiveMonths']
    
    # Data Cleaning
    all_data['Gender'] = pd.Categorical(all_data.Gender).codes
    all_data['ContactAvailability'] = pd.Categorical(all_data.ContactAvailability).codes
    all_data['HouseType'] = pd.Categorical(all_data.HouseType).codes
    all_data['HomeCountry'] = pd.Categorical(all_data.HomeCountry).codes
    all_data_h20 = h2o.H2OFrame(all_data)
    
    train,test = all_data_h20.split_frame([0.8], seed = 123)
    train = train[:,1:11]
    test = test[:,1:11]

    # Loading the H2O model
    print("Processing Step 2 --> Loading the H2O model into the solution")
    estimator = h2o.load_model("..\\..\\02_models\\KMeans_model_python_1537328280878_1")

    trained = estimator.predict(all_data_h20)
    all_data_h20['cluster'] = trained["predict"].asfactor()
    all_data_h20 = all_data_h20.as_data_frame()

    print("Processing Step 3 --> Scoring for a sample customer")
    sample_customer = test[2,:]
    print(sample_customer)
    predicted = estimator.predict(sample_customer)
    print("Predicted Cluster : ",predicted["predict"].asfactor())

    # Calculate average CLV of that cluster
    req_value = int(predicted["predict"].asfactor())
    req_data = all_data_h20[all_data_h20['cluster']==req_value]
    avg_clv = req_data['CLV'].mean()
    print('CLV for new customer = ',str(avg_clv))
    print('Process Complete')
    
    return avg_clv

# Main function starts here
def main():

    # For testing only - Create your own new customer and use that in the function above
    new_customer_details = [211,'Female',108,'Rented','High','Singapore','NULL',0,0,1]
    avg_clv = clv_clustering_scoring(new_customer_details)
    
if __name__ == "__main__":
    main()  
    
