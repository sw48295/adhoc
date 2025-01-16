##########################################################
#               Import Dependencies
##########################################################

import math
import numpy as np
import pandas as pd
import io
import copy
from pandasql import sqldf
import os
import warnings
warnings.filterwarnings("ignore")


def graph2code():
    ##########################################################
    #           Read and clean the Graph data
    ##########################################################

    # Your current directory (change this)
    # Directory = r"C:\Users\ShenliangWu\Downloads\DHL\Decision_Lineage"

    # change directory
    # os.chdir(Directory)

    # Sheet name in Rule2Graph.xlsx (change this)
    # section_sheet = 'Section_4_Mortgage_Statement'
    section_sheet = 'Section_5_Tax_Insurance'
    # section_sheet = 'Section_6_Harzard_Insurance'
    # section_sheet = 'Section_15_1003_Application'


    df = pd.read_excel("Rule2Graph.xlsx", sheet_name = section_sheet)

    # Convert column names to uppercase
    df.columns = [x.upper() for x in df.columns]

    # Modify ID columns
    df['NODE_ID'] = df['NODE_ID'].str.replace("['.','-']","_")
    df['PARENT_ID'] = df['PARENT_ID'].str.replace("['.','-']","_")

    # Sort the data and add parent indices
    df = df.sort_values(by=['PARENT_ID','NODE_ID'], ascending=[True, True])

    # Cumulative count by group 'PARENT_DESC'
    df['CUM_COUNT'] = df.groupby(['PARENT_ID']).cumcount()+1

    # (1) Remove OPERATOR if the rule/sub-rule is the first in the order
    # (2) Rename attribute "NODE_DESC"
    for i in range(len(df)):
        df['OPERATOR'][i] = str(df['OPERATOR'][i]).upper()
        if df['CUM_COUNT'][i] == 1 or df['OPERATOR'][i]== 'NAN':
            df['OPERATOR'][i] = " "
        # if df['NODE_TYPE'][i] == 'raw_attr':
        #     df['NODE_DESC'][i] = str(df['NODE_DESC'][i]) + "_" + str(df['CUM_COUNT'][i])

    # Rearrange columns and drop duplicates
    col_list = ['NODE_ID', 'PARENT_ID', 'NODE_TYPE', 'NODE_DESC', 'PARENT_DESC',  'OPERATOR', 'CUM_COUNT', 'FORMULA']
    df = df[col_list]

    df = df.drop_duplicates(subset=['NODE_TYPE', 'NODE_ID', 'PARENT_ID'], keep="first")


    ##########################################################
    #                       Graph2Code
    ##########################################################

    #### Generate FORMULA for non-leaf rules
    '''
    Below code traverses from root node to leaf rules and generates formulas for all non-leaf rules
    '''
    # Remove raw attributes from the dataframe
    df1 = df[~df["NODE_TYPE"].isin(['raw_attr'])]

    # Root node
    root = ''
    current_parent = [root]
    current_children = df1[df1["PARENT_ID"].isnull()]['NODE_ID'].tolist()

    data = []
    while len(current_children)>0:
        
        # Dataset of children of current_parent
        temp = df1[df1["PARENT_ID"].isin(current_parent)]

        for pt_item in current_parent:
            temp_parent = temp[temp["PARENT_ID"]==pt_item]
            formula = ''
            for index, row in temp_parent.iterrows():

                if row['OPERATOR'] != "":
                    formula = formula + " "+ str(row['OPERATOR']) + " " + str(row['NODE_ID'])
                else:
                    formula = formula +str(row['NODE_ID'])
            data.append((pt_item, formula.strip()))

        current_parent = current_children
        current_children = df1[df1["PARENT_ID"].isin(current_parent)]['NODE_ID'].tolist()     
                    
    # Convert list to dataframe
    data = pd.DataFrame(data, columns=["NODE_ID","FORMULA2"])            

    # Merge FORMULA column with main dataframe
    df1 = pd.merge(df1, data, how = 'left', left_on = ['NODE_ID'], right_on = ['NODE_ID'])

    # Places FORMULAs in one column and drop the extra one
    for i in range(len(df1)):
        if pd.isna(df1["FORMULA"][i])==True:
            df1["FORMULA"][i] = df1["FORMULA2"][i]

    df1 = df1.drop('FORMULA2', axis=1)



    #### Generate SQL query for the section and write to text file
    # Remove raw attributes from the dataframe
    df2 = df1[~df1["NODE_TYPE"].isin(['raw_attr'])]

    # Root node
    root = ''
    current_parent = [root]
    current_children = df2[df2["PARENT_ID"].isnull()]['NODE_ID'].tolist()

    query_text = ''
    temp_query_text = []

    count = 0
    while len(current_children)>0:
        
        # Dataset of children of current_parent
        temp = df2[df2["NODE_ID"].isin(current_children)]


        formula_text = ''
        for index, row in temp.iterrows():
            if row["NODE_TYPE"] == 'leaf_rule':
                formula_text = formula_text + "\n, CASE WHEN "+ str(row['FORMULA'].strip()) + " THEN 1 ELSE 0 END as " + str(row['NODE_ID'])  
            else:
                formula_text = formula_text + "\n, "+ str(row['FORMULA'].strip()) + " as " + str(row['NODE_ID'])  

        temp_text = str("select *") + formula_text + " " + str(" from data") 
        
        temp_query_text.append(temp_text)
        query_text = temp_text + "\n\n" + query_text
        
        current_parent = current_children
        current_children = df2[df2["PARENT_ID"].isin(current_parent)]['NODE_ID'].tolist()

        count += 1 #increase count by 1 in each loop

    # Replace first instance of 'data' in query text with actual dataset name (appr2 in this case) 
    query_text = query_text.replace("from data", "from appr2", 1)
    temp_query_text[count-1] = temp_query_text[count-1].replace("from data", "from appr2", 1)

    with open("query_text.txt", "w") as text_file:
        text_file.write("%s" % query_text)

    # Generate a list of all raw attributes in the section
    raw_attr = list(set(df[df["NODE_TYPE"].isin(['raw_attr'])]["NODE_DESC"].to_list()))

    return(section_sheet, temp_query_text, raw_attr)

# graph2code()