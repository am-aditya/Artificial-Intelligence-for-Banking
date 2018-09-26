# Fraud detection - Rule Based Engine (Sample) and Deep Learning Scoring Model
# Created Date: 04-09-2018
# This is just a sample of the original code and not the original code itself.
# In order to get the full program and learn more about how this works please contact www.justanalytics.com

import pyodbc
import pandas as pd
import os
import sys
import time
import geopy.distance
import numpy as np
from datetime import datetime
import json
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import Dropout
from keras.models import model_from_json
import keras.backend as K
from shutil import copyfile


def insert_to_db(customer_id,trans_amt,trans_dt,trans_country):

    # In case you want to push data to your database, you can write the equivalent code here.
    print("Insertion into database not part of the demo. Falling back to CSV Processing. Proceeding to the next step")

    # Creating a copy of the CSV for usage
    copyfile("..\\..\\99_sample_data\\Sample_CustTransactions_1354564.csv", "..\\..\\99_sample_data\\Sample_CustTransactions_1354564_Temp.csv")
   

def score_fraud(customer_id,trans_amt,trans_dt,trans_country):

    print('Starting Model Scoring')
    
    result = {}

    all_data = pd.read_csv("..\\..\\99_sample_data\\Sample_CustTransactions_1354564_Temp.csv")
    
    # Getting all records
    all_data = all_data[['AccountNo','TransactionAmount','LargePurchase','RiskBucket','TransactionCountry','Product','Gender','Age','ContactAvailability','HouseType','HomeCountry','CreditScore','PotentialFraud']]
    all_data['CreditScore'] = 1
    all_data['RiskBucket'] = 1      
    all_data = all_data.fillna(0)
    
    # Data Cleaning
    all_data['RiskBucket'] = pd.Categorical(all_data.RiskBucket).codes
    all_data['Product'] = pd.Categorical(all_data.Product).codes
    all_data['Gender'] = pd.Categorical(all_data.Gender).codes
    all_data['ContactAvailability'] = pd.Categorical(all_data.ContactAvailability).codes
    all_data['HouseType'] = pd.Categorical(all_data.HouseType).codes
    all_data['HomeCountry'] = pd.Categorical(all_data.HomeCountry).codes
    all_data['TransactionCountry'] = pd.Categorical(all_data.TransactionCountry).codes
    all_data['PotentialFraud'] = pd.Categorical(all_data.PotentialFraud).codes

    # trans_country
    cur_rec = [int(customer_id), int(trans_amt), all_data['LargePurchase'].iloc[0],
               all_data['RiskBucket'].iloc[0], int(1) , all_data['Product'].iloc[0],
               all_data['Gender'].iloc[0], all_data['Age'].iloc[0], all_data['ContactAvailability'].iloc[0],
               all_data['HouseType'].iloc[0], all_data['HomeCountry'].iloc[0], all_data['CreditScore'].iloc[0]]
    
    ############################# load json and create model
    json_file = open('..\\..\\02_models\\fraud_dl_model.json', 'r')
    loaded_model_json = json_file.read()
    json_file.close()
    loaded_model = model_from_json(loaded_model_json)
    # load weights into new model
    loaded_model.load_weights('..\\..\\02_models\\fraud_dl_model.h5')
    print("Loaded model from disk")
     
    # evaluate loaded model on test data
    loaded_model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
    #########################

    cur_rec = pd.DataFrame([cur_rec])
    prediction = loaded_model.predict_classes(cur_rec)
    predic_prob = loaded_model.predict_proba(cur_rec)
    print('Predicted Fraud = ',prediction[0][0])
    print('Predicted Probability = ',predic_prob[0][0])
    result['Prediction'] = prediction[0][0]
    result['PredictionProb'] = predic_prob[0][0]

    return result

def fraud_rule_check(customer_id,trans_amt,trans_dt,trans_country):

    # Initializing variables
    potentialFraud = 0
    fraudReason = []

    trans_data = pd.read_csv("..\\..\\99_sample_data\\Sample_CustTransactions_1354564_Temp.csv")

    # Rule-1 - If country different from list of transacted countries
    all_trans_countries = trans_data.TransactionCountry.unique()
    if(trans_country not in all_trans_countries):
        potentialFraud = potentialFraud + 0.1
        fraudReason.append('Transaction in a new country')

    # Rule-2 - If distance of transaction is greater than 5000
    coords_1 = (1.3521, 103.8198) # Singapore -  Insert lat long of previous country. Hardcoding for the demo.
    coords_2 = (33.9391, 67.7100) # Afghanistan - Insert lat long of current country selected. Hardcoding for the demo.
    # In a real scenario this would be picked up from the CountryMaster Table in the database

    distance_cur_prev_trans = geopy.distance.vincenty(coords_1, coords_2).km

    if(distance_cur_prev_trans > 5000):
        potentialFraud = potentialFraud + 0.1
        fraudReason.append('Transaction in a country greater than average distance moved previously')

    # Rule-3 - Time between two transactions and distance
    last_trans_data = trans_data.tail(1)
    last_trans_time = last_trans_data.TransactionDate
    last_trans_time = str(last_trans_time.values[0])
    print(last_trans_time)
    print(type(last_trans_time))
    print((trans_dt))
        
    f = '%Y-%m-%d %H:%M:%S' # time format
    last_trans_time = datetime.strptime(last_trans_time,f)
    trans_dt = datetime.strptime(trans_dt,f)
    
    time_diff = trans_dt - last_trans_time
    time_diff_mins = (time_diff.days * 86400 + time_diff.seconds)/60
    
    if (distance_cur_prev_trans > 2000 and time_diff_mins < 100):
        potentialFraud = potentialFraud + 0.6
        fraudReason.append('Transaction in multiple countries in a short duration')

    # Rule-4 - Anomalous transaction amount
    all_trans_amount = trans_data.TransactionAmount
    trans_amt_avg = np.mean(all_trans_amount)

    if(trans_amt > 5*trans_amt_avg):
        potentialFraud = potentialFraud + 0.2
        fraudReason.append('Anomalous transaction amount')

    print('Potential Fraud : {}'.format(str(potentialFraud)))
    print('Fraud Reason : {}'.format(fraudReason))

    # Creating the JSON object to return
    final_fraud_json = {}
    final_fraud_json['potentialFraud'] = potentialFraud
    final_fraud_json['fraudReason'] = fraudReason
    final_fraud_json = json.dumps(final_fraud_json)
   
    return final_fraud_json

# Main function starts here
def main():

    # Sample JSON Input. You can modify this to see different results. Do not change the cust_id since the sample data is limited to it.
    input_json = {"User_ID":1100222,
                  "purchased_vendor":"CINNAMON GRAND",
                  "purchased_amount":10,"trans_dt":"2018-09-19 10:22:57",
                  "cust_id":1354564,"trans_cnt":"Afghanistan","trans_card":"American Express Coporate Green Card"}
    
    # The main function is used to run and test a sample input
    # Extract from JSON file coming in later
    customer_id = input_json['cust_id']
    trans_amt = input_json['purchased_amount']
    trans_dt = input_json['trans_dt']
    trans_country = input_json['trans_cnt']
    card_type = input_json['trans_card']
    vendor_name = input_json['purchased_vendor']

    insert_to_db(customer_id,trans_amt,trans_dt,trans_country) # Inserting current transaction into the db

    re_json = fraud_rule_check(customer_id,trans_amt,trans_dt,trans_country) # Rule engine for fraud
    ft_output = score_fraud(customer_id,trans_amt,trans_dt,trans_country) # Transaction fraud detection
           
    trans_date = datetime.strptime(trans_dt,'%Y-%m-%d %H:%M:%S').date()
    trans_time = datetime.strptime(trans_dt,'%Y-%m-%d %H:%M:%S').time()
    
    # (33.9391, 67.7100) # Afghanistan - Insert lat long of current country selected. Hardcoding for the demo. 
    trans_country_lat = 33.9391
    trans_country_long = 67.7100
    re_json = json.loads(re_json)

    re_json['fraudReason'] = ",".join(re_json['fraudReason'])
    print(re_json['fraudReason'])

    # Calculating Fraud Probability
    final_fraud_prob = 0.0
    final_fraud_prob = float(ft_output['PredictionProb']) + float(re_json['potentialFraud'])

    if(final_fraud_prob > 0.4): # Threshold set to 0.4 - It can be changed
        final_fraud_prob_flag = 1
    else:
        final_fraud_prob_flag = 0

    # Creating the final JSON
    final_json = {"trans_rec_details": [
        {
        "fraud_1": {
                "cust_id": customer_id,
                "vendor": vendor_name,
                "card_type": card_type,
                "txAmt": trans_amt,
                "txCntry": trans_country,
                "txLat": trans_country_lat,
                "txLong": trans_country_long,
                "txDate": str(trans_date),
                "txTime": str(trans_time),
                "fraudFlag": final_fraud_prob_flag,
                "fraudProb": final_fraud_prob,
                "fraudReason": re_json['fraudReason'],
                "fraudSMS": "A transaction of $" + str(trans_amt) + " was made on your credit card. Was this transaction done by you? Reply 'Yes' or 'No' to confirm."
                }
            }
        ]
    }
    
    try:
        import os
        os.remove('fraud_out.json')
    except:
        print('')

    # Creating the final JSON file
    with open('fraud_out.json', 'w+') as f:
        f.write(json.dumps(final_json, indent=4, sort_keys=False))
    f.close()

    # Printing the final results
    print(json.dumps(final_json, indent=4, sort_keys=False))
    os.remove("..\\..\\99_sample_data\\Sample_CustTransactions_1354564_Temp.csv") # Removing the temp file created
    
    K.clear_session()

if __name__ == "__main__":
    main()
