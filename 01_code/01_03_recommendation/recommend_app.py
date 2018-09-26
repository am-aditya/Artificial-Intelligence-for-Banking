#!/usr/bin/env python3

# General Imports
import re
import sys
import json
import time
import random
import string 
import pyodbc
import pandas
import random
# import logging  
# import argparse
import urllib.request

# Package namespace import
# from time import strftime
from datetime import datetime

# Specific functions/classes import
from pathlib import Path
from configparser import SafeConfigParser

class RecommendPL(object):
    """Recommendation Product Line app

    For Recommendation App demo application

    """

    def __init__(self, cfg_file=None, logger=None):
        """Initialization
        Input:
            cfg_file: Path to config file. Looks for default file if not provided.
            logger: Specfic logger object to write to. Else it will get a logger. If no parent logger exist, a log file will be
            created for each instance of this class.

        """
        
        # logger 
        # self.logger = logger or logging.getLogger('jarecom.recommendpl')
        # if not self.logger.hasHandlers():
        #     # No handlers found from this logger or its parents. Creates a log file for each instance for this class.
            
        #     # Create a file handler
        #     file_handler = logging.FileHandler("jarecom_"+ time.strftime("%Y%m%d-%H%M%S")+".log")
        #     file_handler.setLevel(logging.DEBUG)
        #     file_handler.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-5s %(message)s'))

        #     self.logger.setLevel(logging.DEBUG)
        #     self.logger.addHandler(file_handler)

        # self.logger.info("Creating RecommendPL instance.")

        # Use default config file name if not provided
        # self.logger.info("Read in config file")
        if not cfg_file:
            # Default config file
            cfg_file = "..\\07_recommendation\\default.cfg"

        # ConfigParser doesn't check if file exists
        if not Path(cfg_file).exists():
            print("Config file {} does not exist! Exiting ...".format(cfg_file))
            # self.logger.error("Config file {} does not exist!".format(cfg_file))
            # sys.exit(1)

        # read config files
        self.config = SafeConfigParser()
        self.config.read(cfg_file)

        # Get DB connection variables
        # You need to update the default config file to use the code further.
        self.server = self.config.get('database','server')
        self.database = self.config.get('database','database')
        self.username = self.config.get('database','user')
        self.password = self.config.get('database','password')
        self.driver= '{SQL Server}'

    def __callModel_old(self, input):
        
        # self.logger.debug("In __callModel(), buiding dictionary for submission to model")
        # Query the transaction table to get the latest transaction for the user
        cnxn = pyodbc.connect('DRIVER='+self.driver+';SERVER='+self.server+';PORT=1443;DATABASE='+self.database+';UID='+self.username+';PWD='+ self.password)
        cursor = cnxn.cursor()
        query = "SELECT TOP 1 [TxnUniqueKey],[AccountNo],[AccountOpenDate],[CardStatusCode],[Age],[Gender],[MaritialStatus],[AccountLimit],[Product],[Segment],[EffectiveDate] FROM [dbo].[NTB_DER_FEATURE_EXTRACTION] WHERE lag_merchantCategory is NULL AND AccountNo = {}".format(input['User_ID'])

        cursor.execute(query)
        row = cursor.fetchone()
        # self.logger.debug(query)
        # self.logger.debug("Result: {}".format(row))

        if datetime.today().weekday() < 5 :
            wk_seg = "WEEKDAY"
        else:
            wk_seg = "WEEKEND"

        # get merchant category for current transaction
        query = "SELECT TOP 1 MerchantCategory, a.location_id, b.longitude, b.latitude from [dbo].[MerchantMaster] as a JOIN [dbo].[MerchantLocation] as b ON a.location_id = b.location_id WHERE Merchant like '%{}%'".format(input["purchased_vendor"])
        cursor.execute(query)
        row2 = cursor.fetchone()
        merc_cat = row2[0]
        # self.logger.debug(query)
        # self.logger.debug("Result:{}".format(merc_cat))
        input['longitude'] = row2[2]
        input['latitude'] = row2[3]
        
        # data is a json with the schema needed for the model web servie
        data = {
            "Inputs": {
                "input1": [
                    {
                        # User information
                        'AccountOpenDate': str(row[2]),   
                        'CardStatusCode': row[3],   
                        'Age': row[4],   
                        'Gender': row[5],   
                        'MaritialStatus': row[6],   
                        'AccountLimit': row[7],   
                        'Product': row[8],   
                        'Segment': row[9], 
                        # Calculate from last query and current transaction. However fixed val for demo
                        'date_diff': "0",   
                        'Week_Segment': wk_seg,
                        # Current transaction amount
                        'TransactionAmount': float(input["purchased_amount"]),
                        # Category of the current transaction merchant   
                        'MerchantCategory': merc_cat,   
                        # Keeping this fixed for now
                        'TransactionType': "Sales Draft",   
                        'TxnClassifAsPerCardUser': "On Us-Sales Draft",   
                        # 'TransactionDescription': "NORMAL SE - Daily",   
                        # 'PaymentMethod': "CREDITING THE ACCT (ACCT TRANSFER)",   
                        # This is the outcome category
                        'lag_merchantCategory': "",   
                    }
                ],
            },
            "GlobalParameters":  {
            }
        }

        # self.logger.debug("Input data to ML request:")
        # self.logger.debug(json.dumps(data, indent=4, sort_keys=False))
        # print(data)

        body = str.encode(json.dumps(data))

        # Machine learning scoring model API details here. Please contact us to know more about this.
        url = 'xxxxx'
        api_key = 'xxxx'
        headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}
        req = urllib.request.Request(url, body, headers)

        # Output array
        out = []
        try:
            # self.logger.debug("In __callModel(), Calling model")
            # Time of api call
            call_time = str(datetime.now())
            response = urllib.request.urlopen(req)
            result = response.read()

            tmp = json.loads(result.decode("utf-8"))
            tmp_1 = tmp['Results']['output1'][0]

            for key in tmp_1.keys():
                # print(key) 
                if re.match('^Scored Probabilities', key) and float(tmp_1[key]) > 0.0: 
                    tmp_key = key.replace("Scored Probabilities for Class ","").replace('"','')

                    # rand_id = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
                    rand_id = 'TestRec'
                    out.append({
                        "confidence" : float(tmp_1[key])*100,
                        "recommendation_time": call_time,
                        "recommendation_id" : rand_id,
                        "recommended_vendor" : tmp_key,
                        "recommended_product" : tmp_key
                    })

            # Sort the recommendations on desc confidence scores
            if len(out) > 1:
                out = sorted(out, key=lambda k: k['confidence'], reverse=True) 

        except urllib.error.HTTPError as error:
            print("Web Service Error!")
            # self.logger.error("The request failed with status code: " + str(error.code))
            # # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
            # self.logger.error(error.info())
            print(error.code)
            print(error.info())
            print(error.read())
            # self.logger.error(json.loads(error.read().decode("utf8", 'ignore')))

        return out[:5], input

    def __callModel(self, input):
        
        # self.logger.debug("In __callModel(), buiding dictionary for submission to model")
        # Query the transaction table to get the latest transaction for the user
        cnxn = pyodbc.connect('DRIVER='+self.driver+';SERVER='+self.server+';PORT=1443;DATABASE='+self.database+';UID='+self.username+';PWD='+ self.password)
        cursor = cnxn.cursor()
        query = "SELECT TOP 1 [TxnUniqueKey],[AccountNo],[AccountOpenDate],[CardStatusCode],[Age],[Gender],[MaritialStatus],[AccountLimit],[Product],[Segment],[EffectiveDate] FROM [dbo].[NTB_DER_FEATURE_EXTRACTION] WHERE lag_merchantCategory is NULL AND AccountNo = {}".format(input['User_ID'])

        cursor.execute(query)
        row = cursor.fetchone()
        # self.logger.debug(query)
        # self.logger.debug("Result: {}".format(row))

        if datetime.today().weekday() < 5 :
            wk_seg = "WEEKDAY"
        else:
            wk_seg = "WEEKEND"

        # get merchant category for current transaction
        query = "SELECT TOP 1 MerchantCategory, a.location_id, b.longitude, b.latitude from [dbo].[MerchantMaster] as a JOIN [dbo].[MerchantLocation] as b ON a.location_id = b.location_id WHERE Merchant like '%{}%'".format(input["purchased_vendor"])
        cursor.execute(query)
        row2 = cursor.fetchone()
        merc_cat = row2[0]
        # self.logger.debug(query)
        # self.logger.debug("Result:{}".format(merc_cat))
        # input['longitude'] = row2[2]
        # input['latitude'] = row2[3]
        
        data = {
            "Inputs": {
                "input1": {
                    "ColumnNames": [
                    "AccountOpenDate",
                    "CardStatusCode",
                    "Age",
                    "Gender",
                    "MaritialStatus",
                    "AccountLimit",
                    "Product",
                    "Segment",
                    "TransactionType",
                    "TransactionAmount",
                    "TxnClassifAsPerCardUser",
                    "MerchantCategory",
                    "date_diff",
                    "Week_Segment",
                    "lag_merchantCategory"
                    ],
                "Values": [
                    [
                    str(row[2]),
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    "Sales Draft",
                    float(input["purchased_amount"]),
                    "On Us-Sales Draft",
                    merc_cat,
                    "0",
                    wk_seg,
                    ""
                    ]
                    ]
                }
            },
            "GlobalParameters": {}
        }
        # self.logger.debug("Input data to ML request:")
        # self.logger.debug(json.dumps(data, indent=4, sort_keys=False))
        # print(data)

        body = str.encode(json.dumps(data))
        # Model built from Azure SQL DB
        # Please enter the API credentials here. To know more about this please get in touch with us
        url = 'xxxxx'
        api_key = 'xxxxx'
        headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}
        req = urllib.request.Request(url, body, headers)

        # Output array
        out = []
        try:
            # self.logger.debug("In __callModel(), Calling model")
            # Time of api call
            call_time = str(datetime.now())
            response = urllib.request.urlopen(req)
            result = response.read()

            tmp = json.loads(result.decode("utf-8"))

            col_names = tmp['Results']['output1']['value']["ColumnNames"]
            col_values = tmp['Results']['output1']['value']["Values"][0]
            tmp_1 = dict(zip(col_names, col_values))

            for key in tmp_1.keys():
                # print(key) 
                if re.match('^Scored Probabilities', key) and abs(float(tmp_1[key])) >= 0.0: 
                    tmp_key = key.replace("Scored Probabilities for Class ","").replace('"','')

                    # Get random merchant using tmp_key
                    query = "SELECT [MerchantLegalName] from [recommendationdb].[dbo].[NTB_DER_FEATURE_EXTRACTION] WHERE MerchantCategory = '{}'".format(tmp_key)
                    cursor.execute(query)
                    all_merchants = cursor.fetchall()
                    tmp_merchant = all_merchants[random.randint(0,len(all_merchants))][0]

                    # rand_id = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
                    rand_id = random.randint(1,500)
                    query2 = "SELECT longitude, latitude from [dbo].[MerchantLocation] WHERE location_id={}".format(rand_id)
                    cursor.execute(query2)
                    row2 = cursor.fetchone()

                    out.append({
                        "confidence" : float(tmp_1[key])*100,
                        "recommendation_time": call_time,
                        "recommendation_id" : rand_id,
                        "recommended_vendor" : tmp_key,
                        "recommended_product" : tmp_merchant,
                        "longitude" : row2[0],
                        "latitude" : row2[1]
                    })

            # Sort the recommendations on desc confidence scores
            if len(out) > 1:
                out = sorted(out, key=lambda k: k['confidence'], reverse=True) 

        except urllib.error.HTTPError as error:
            print("Web Service Error!")
            # self.logger.error("The request failed with status code: " + str(error.code))
            # # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
            # self.logger.error(error.info())
            print(error.code)
            print(error.info())
            print(error.read())
            # self.logger.error(json.loads(error.read().decode("utf8", 'ignore')))

        return out[:5], input

    def __getOffer(self, recommend):
        """ Get offer based on recommended vender ?and product?
        Input: 
            Dictionary from the __callModel() function

        Output:
            Offer from the offers table in a dictionary

        """
        # self.logger.debug("In __getOffer(), buiding dictionary for submission to model")
        # Query the transaction table to get the latest transaction for the user
        cnxn = pyodbc.connect('DRIVER='+self.driver+';SERVER='+self.server+';PORT=1443;DATABASE='+self.database+';UID='+self.username+';PWD='+ self.password)
        # cursor = cnxn.cursor()
        query = "SELECT a.MerchantCategory, b.* FROM [dbo].[OffersByMerchantCategory] as a JOIN [dbo].[OffersMaster] as b ON a.OfferID = b.OfferID WHERE a.MerchantCategory = '{}'".format(recommend['recommended_vendor'])
        # self.logger.debug(query)

        data = pandas.read_sql(query,cnxn)
        # self.logger.debug("Result: {}".format(data))
        
        
        # # Get the industry from the recommendation
        # vendor = recommend['recommended_vendor'].lower()

        # # Dummy offer table
        # recom_txt = {
        #     "drug stores & pharmacies" : "Buy 2 Multi-vitamins bottles and get 1 free. Terms and conditions apply",
        #     "utilities-electric" : "15$ off on your bill payment",
        #     "restaurants" : "5$ off on next two transactions",
        #     "grocery stores/supermarkets" : "5$ off on your next transaction with a minimum purchase of 50$",
        #     "fuel dealers" : "5$ off non-fuel purchases on your next transaction"
        # }

        if not data.empty:
            rand_idx = random.randint(0, len(data.index)-1)
            
            out_dict = {
                'recommendation' : data.loc[rand_idx,'Offer'],
                'recommended_voucher_code' : int(data.loc[rand_idx,'OfferID']),# numpy returns this as int64, not json serializable
                'recommended_expiry' : data.loc[rand_idx,'OfferValidity'],
                'url' : data.loc[rand_idx,'OfferLink']
            }
        else:
            out_dict = {}

        return out_dict

    def __getComm(self, curr_sale, recom_sale, offer_dict):
        """Creates full offer message for SMS
        Input:
            Dicts containing current transaction, a recommendation, corresponding offer.

        """
        # Create the SMS txt to be sent
        out_txt = "Hi! Based on your previous purchase at {} for ${}, you have been offered a voucher for {} at {} for {}. This offer expires on {:%d %B %Y}.".format(
            curr_sale['purchased_vendor'],
            curr_sale['purchased_amount'],
            offer_dict['recommendation'],
            recom_sale['recommended_vendor'].title(),
            recom_sale['recommended_product'],
            datetime.strptime(offer_dict['recommended_expiry'],'%d/%m/%Y'),
        )

        out_txt += "To know more visit {}".format(offer_dict['url'])

        return out_txt

    def getRecommendations(self, input_ml):
        """Only public method for this class

        Input:
            input_ml : Input from web app as a dictionary. Required keys are: ["User_ID", "purchased_vendor", "purchased_amount": "100"]. Any
            other keys are not used but is copied into the output.

        Output:

        """
        
        # self.logger.debug("Input to ml: {}".format(input_ml))

        # Get top N recommendations from model. Responses given in an array
        results, input_ml = self.__callModel(input_ml)
        # self.logger.debug("Results: {}".format(results))

        out_dict = {} # Each recommendation has its own key rather than as an element in a list
        # For each recommendation, 
        count = 1
        for result in results:
            # Get the offer
            offer = self.__getOffer(result)
            if not offer:
                continue

            # self.logger.debug("Offer: {}".format(offer))
        
            # Create the SMS/Communication message
            if offer:
                mesg = self.__getComm(input_ml, result, offer)
            else:
                mesg = ""
            # self.logger.debug("Mesg: {}".format(mesg))

            # Combine dictionaries
            # https://stackoverflow.com/questions/38987/how-to-merge-two-dictionaries-in-a-single-expression
            this_dict = {
                **input_ml,
                **result, 
                **offer,
                "SMS" : mesg}

            # print(json.dumps(this_dict, indent=4, sort_keys=False))
            out_dict["Recommendation_{}".format(count)] = this_dict
            count += 1

        return {
            "trans_rec_details": [
                out_dict
            ]
        }
        

def old_main(): # Sample
    """App to test the class
    """
    import random 

    # Start 
    # - Setup logging 
    # logger = ja.create_logger('jarecom') # Create the logger. Filename is name_yyyymmdd.log
    # logger.setLevel(logging.ERROR)
    # logger.newline()
    # logger.info("Start of JA Recommendation App")

    # #############################################################################################
    accounts = [1304878] #1198492]#, 1210626, 1526916, 1280849,1286220]
    vendors = ['LANKA FILLING STATION','KEELLS SUPER','DIALOG','CINNAMON', 'ODEL', ]
    # categories = [FUEL DEALERS or PETROLEUM/PETROLEUM PRODUCTS, GROCERY STORES/SUPERMARKETS,RESTAURANTS, "Utilities-Electric, HOTELS/MOTELS/RESORTS, FAMILY CLOTHING STORES ]
    amounts = [2000, 5000, 10000, 15000, 20000]

    input_trans = {
        # "User_ID": "jon_mayer",
        "User_ID": random.choice(accounts),
        "purchased_vendor": random.choice(vendors),
        "purchased_amount": random.choice(amounts),
        # "StoreID" : "",
        # "lat": "",
        # "long": ""
    }

    rec = RecommendPL().getRecommendations(input_trans)
    
    print(json.dumps(input_trans, indent=4, sort_keys=False))
    print(json.dumps(rec, indent=4, sort_keys=False))

    # #############################################################################################

    # Housekeeping
    # - Remove any intermediate file (if any)
    
    # logger.info("End of Processing") # Close logging


def main():
    
    input_trans = json.loads(sys.argv[1])

    rec = RecommendPL().getRecommendations(input_trans)
    # result = {'status': 'Yes!'}
    # print(json.dumps(result))
    print(json.dumps(rec, indent=4, sort_keys=False))

if __name__ == "__main__":
   old_main()

