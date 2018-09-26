# The program is a sample to perform the survival analysis for the customers across certain features
# This program uses the simple Kaplan-Meyer Tests to perform this analysis
# Created On : 28-08-2018

import os
import sys
import time
import pandas as pd
import numpy as np
import pyodbc
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches #for custom legends
import seaborn as sns
from lifelines import KaplanMeierFitter #survival analysis library
from lifelines.statistics import logrank_test #survival statistical testing

sys.__stdout__ = sys.stdout

# Reading in sample data
all_data = pd.read_csv("..\\..\\99_sample_data\\Sample_CustChurnData.csv") # getting all the data
all_data['Churn'] = all_data.MonthsSinceLastTrans.apply(lambda x: 1 if x > 6 else 0) #recode churn var
#all_data['Product'] = pd.Categorical(all_data.Product).codes
print(all_data.head())

# Data Cleaning
all_data = all_data.fillna(0)
print(all_data.shape)
df = all_data

kmf = KaplanMeierFitter()
T = df['TenureIndays'] #duration
E = df['Churn'] # 1 if death/churn is seen, else 0

ax = plt.subplot(111)

# Sample for cards
t = np.linspace(0, 250, 51)
for key in df.Product.unique():
    dem = (df["Product"] == key)
    kmf.fit(T[dem], event_observed=E[dem], timeline=t, label=key)
    ax = kmf.plot(ax=ax,ci_show=False)

plt.title('Survival function');
box = ax.get_position()
ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])

# Put a legend to the right of the current axis
ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

plt.ylim(0,1)
plt.title("Lifespans of different cards");
plt.ylabel('Probability')

plt.show()



