##########################################################
#               Import Dependencies
##########################################################

import math
import numpy as np
import pandas as pd
import io
import copy
from pandasql import sqldf
import re
import os
import warnings
warnings.filterwarnings("ignore")

# Your current directory (change this)
Directory = r"C:\Users\ShenliangWu\Downloads\DHL\Decision_Lineage"

# change directory
os.chdir(Directory)

##########################################################
#           Read and clean the Graph data
##########################################################

# Sheet name in XXXX.xlsx from Smartdraw (change this)
section_sheet = 'Section_5_Tax_Insurance'
# section_sheet = 'Section_6_Harzard_Insurance'
# section_sheet = 'Section_15_1003_Application'


df = pd.read_excel(section_sheet + ".xlsx", sheet_name = section_sheet)

# Drop columns that are not useful
df = df.drop(['Shape', 'Color'], axis=1) 

Node_ID, Node_Desc = [], []
Parent_ID = []

# Extract Node_ID, Parent_ID and Node_Desc from regex patterns and create columns in the dataset
for values in df['Name']:
    try:
        Node_ID.append(re.search(r'(R_[0-9_]+)', values).group())  
        Node_Desc.append(values.replace(re.search(r'(R_[0-9_]+)', values).group(), '')) 
    except:
        Node_ID.append('')
        Node_Desc.append('')

for values in df['Parent']:
    try:
        Parent_ID.append(re.search(r'^(R_[0-9_]+)', values).group())
    except:
        Parent_ID.append('')

df['Node_ID'] = Node_ID
df['Parent_ID'] = Parent_ID
df['Node_Desc'] = Node_Desc


# If Node_ID and Node_Desc are missing (since they dont match regex pattern - in case of raw_attr), replace them with raw_attr name
for i in range(len(df)):
    if df['Node_Desc'][i] == '':
        df['Node_Desc'][i] = df['Name'][i]

    if df['Node_ID'][i] == '':
        df['Node_ID'][i] = df['Name'][i]


df['Node_Desc'] = df['Node_Desc'].str.strip()

# Convert column names to uppercase
df.columns = [x.upper() for x in df.columns]

# Modify ID columns
df['NODE_ID'] = df['NODE_ID'].str.replace("['.','-']","_")
df['PARENT_ID'] = df['PARENT_ID'].str.replace("['.','-']","_")

# Remove all leading and trailing spaces from ID columns
df['NODE_ID'] = df['NODE_ID'].str.strip()
df['PARENT_ID'] = df['PARENT_ID'].str.strip()

# Add 'PARENT_DESC' column from 'NODE_DESC'      
df = pd.merge(df, df[['NODE_ID', 'NODE_DESC']], how = 'left', left_on = ['PARENT_ID'], right_on = ['NODE_ID'])
# rename columns
df = df.rename(columns={'NODE_ID_x':'NODE_ID', 'NODE_ID_y':'PARENT_ID_NEW', 'NODE_DESC_x':'NODE_DESC', 'NODE_DESC_y':'PARENT_DESC', 'DECISION': 'OPERATOR'})

df.loc[df["PARENT_ID_NEW"]=='', "PARENT_DESC"] = ''

df = df.drop_duplicates(subset=['NODE_ID', 'PARENT_ID'], keep="first")


####################################################################################
# Requirement-1: auto create node_type
####################################################################################

#### step-1: Generate 'RULE_TYPE' whether non_raw_attr/raw_attr
'''
Below code traverses from root node to raw attributes
'''
df1 = df.copy()

# Root node
root = ''
current_parent = [root]
current_children = df1[df1["PARENT_ID"]=='']['NODE_ID'].tolist()

data = []
while len(current_children)>0:
    
    # Dataset of children of current_parent
    temp = df1[df1["PARENT_ID"].isin(current_parent)]

    for pt_item in current_parent:
        temp_parent = temp[temp["PARENT_ID"]==pt_item]
        rule_type = ''
        for index, row in temp_parent.iterrows():

            rule_type = "non_raw_attr"
        data.append((pt_item, rule_type))
        

    current_parent = current_children
    current_children = df1[df1["PARENT_ID"].isin(current_parent)]['NODE_ID'].tolist()     
                
# Convert list to dataframe
data = pd.DataFrame(data, columns=["NODE_ID", "RULE_TYPE"])            

# Merge RULE_TYPE column with main dataframe
df1 = pd.merge(df1, data, how = 'left', left_on = ['NODE_ID'], right_on = ['NODE_ID'])


#### step-2: Generate 'NODE_TYPE' column (Label: master, raw_attr, decision_node)

df1["NODE_TYPE"] = ''
for i in range(len(df1)):
    
    if df1["RULE_TYPE"][i] == "non_raw_attr":
        df1["NODE_TYPE"][i] = "decision_node"
    else:
        df1["NODE_TYPE"][i] = "raw_attr"
    
    if df1["PARENT_ID"][i]=='':
        df1["NODE_TYPE"][i] = "master"

# Label "leaf_rules" (parents of 'raw_attr' are 'leaf_rule')
for i in range(len(df1)): 
    for j in range(len(df1)):
        if df1["NODE_TYPE"][i] == "raw_attr":
            if df1["NODE_ID"][j] == df1["PARENT_ID"][i]:
                df1["NODE_TYPE"][j] = "leaf_rule"



#### step-3: Generate 'FORMULA' for non-leaf rules
'''
Below code traverses from root node to leaf rules
'''

# Root node
root = ''
current_parent = [root]
current_children = df1[df1["PARENT_ID"]=='']['NODE_ID'].tolist()

data = []
while len(current_children)>0:
    
    # Dataset of children of current_parent
    temp = df1[df1["PARENT_ID"].isin(current_parent)]

    for pt_item in current_parent:
        temp_parent = temp[temp["PARENT_ID"]==pt_item]
        formula = ''
        for index, row in temp_parent.iterrows():
            if row['NODE_TYPE']=='raw_attr':
                if pd.isna(row['OPERATOR'])==False:
                    formula = formula + " "+ str(row['OPERATOR']) + " " + str(row['NODE_ID']) + " =1" 
                else:
                    formula = formula +str(row['NODE_ID']) + " =1"
            else:
                if pd.isna(row['OPERATOR'])==False:
                    formula = formula + " "+ str(row['OPERATOR']) + " " + str(row['NODE_ID'])
                else:
                    formula = formula +str(row['NODE_ID'])

        data.append((pt_item, formula.strip()))
        

    current_parent = current_children
    current_children = df1[df1["PARENT_ID"].isin(current_parent)]['NODE_ID'].tolist()     
                
# Convert list to dataframe
data = pd.DataFrame(data, columns=["NODE_ID","FORMULA"])            

# Merge FORMULA column with main dataframe
df1 = pd.merge(df1, data, how = 'left', left_on = ['NODE_ID'], right_on = ['NODE_ID'])

# drop the extra columns
df1 = df1.drop(['RULE_TYPE'], axis=1) 


df1.to_csv("Test1.csv", index = False)