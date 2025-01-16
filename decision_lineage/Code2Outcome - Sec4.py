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


##########################################################
#           Read and clean the loan application data
##########################################################

# Your current directory (change this)
Directory = r"C:\Users\ShenliangWu\Downloads\DHL\Decision_Lineage"

# change directory
os.chdir(Directory)


# Reads the excel file with approved and declined loans into two pandas dataframes
appr = pd.read_excel("Rules_Attr_Digitization_new.xlsx", sheet_name = '2.Raw_attr_100_aprd_app_PostPro', skiprows=0, index_col=None, na_values=['NA'])
dec = pd.read_excel("Rules_Attr_Digitization_new.xlsx", sheet_name = '2.Raw_attr_100_dec_app_PostPro', skiprows=0, index_col=None, na_values=['NA'])
appr1 = appr.drop_duplicates(subset='Attribute_name_for_Python_Code',keep='first')

dec1 = dec.drop_duplicates(subset='Attribute_name_for_Python_Code',keep='first')


# create a list with var names for approved loans
var_list = ['Attribute_name_for_Python_Code','Loan-0','Loan-1_Refined']
for i in range(2,106):
    var_list.append(str('A_Loan{}'.format(i)))

# remove loans with missing data from the sample
var_list.remove('A_Loan17')
var_list.remove('A_Loan27')
var_list.remove('A_Loan28')
var_list.remove('A_Loan51')
var_list.remove('A_Loan81')
var_list.remove('A_Loan82')

# create a list with var names for declined loans
var_list_dec = ['Attribute_name_for_Python_Code']
for i in range(1,101):
    var_list_dec.append(str('D_Loan{}'.format(i)))

# subset the approved loans dataset    
appr1 = appr1[var_list]
appr2 = appr1.set_index('Attribute_name_for_Python_Code').T # transpose

appr2.columns = appr2.columns.str.replace("[';','.']", "") # replace special characters from column names
appr2.columns = appr2.columns.str.replace("[#,@,&,' ',',',(,),<,>,/,?]", "_")  # replace special characters from column names

# subset the declined loans dataset    
dec1 = dec1[var_list_dec]
dec2 = dec1.set_index('Attribute_name_for_Python_Code').T # transpose

dec2.columns = dec2.columns.str.replace("[';','.']", "") # replace special characters from column names
dec2.columns = dec2.columns.str.replace("[#,@,&,' ',',',(,),<,>,/,?]", "_")  # replace special characters from column names

appr2 = appr2.append(dec2)


##### Add attributes that are missing in the raw data
# Sec-1
appr2['application_date'] =  '2022-08-16'

# Sec-4
appr2['lien_payment_adjustment_date'] =  '2022-12-01'
appr2['adjustment_date'] = '2022-12-01'
appr2['Mortgage_Statement_Date'] = '2022-08-01'
appr2['payment_updated_2_pct_above_existing_rate_1st_lien'] =  1
appr2['payment_updated_in_DTI_calc'] = 1
appr2['late_fee_owed'] = 1
appr2['mortgage_stmt_indicates_escrows'] = 1
appr2['taxes_insurance_captures_in_NetO'] = 1
appr2['evidence_of_mtgt_ins_coverage_shortage'] = 1
appr2['rate_adjustment_within_3y'] = 1
appr2['deferred_balance_CLTV'] = 1
appr2['All_fields_in_MS_complete'] = 1
appr2['verify_stmnt_contains_all_pages'] = 1
appr2['ineligible_1st_lien_type_if_DHL_is_2nd_lien'] = 1
appr2['has_escrow'] = 1
appr2['negative_escrow_bal'] = 1
appr2['subject_property_address_on_MS'] = 'ABC'
appr2['address_on_REO'] = 'ABC'
appr2['address_on_1003'] = 'ABC'
appr2['mailing_address_on_MS'] = 'ABC'
appr2['mailing_address_on_1003'] = 'ABC'
appr2['borrowers_on_MS'] = 'ABC'
appr2['borrowers_on_1003'] = 'ABC'
appr2['pymt_on_MS'] = 'X'
appr2['payment_on_credit_report'] = 'X'
appr2['valid_lender_in_MS'] = 1
appr2['stmnt_Not_edited_modified'] = 1
appr2['Mtgt_Stmnt_indicate_property_or_project_type'] = 1
appr2['mtgt_stmnt_is_full_month_pymt'] = 1
appr2['os_bal_on_MS'] = 'X'
appr2['os_bal_on_credit_report'] = 'X'
appr2['current_month_late_payment_on_mtgt_stmnt'] = 0
appr2['curr_NOD_or_PreForeclosure_evidence'] = 1
appr2['has_forebearance'] = 0
appr2['mtgt_on_mtgt_stmnt_match_mtgt_on_creditreport'] = 1
appr2['proof_of_payment'] = 1
appr2['proof_of_payment_date'] = '2022-06-01'
appr2['proof_of_payment_credit_card'] = '2022-06-01'

appr2['ARM_ind'] = appr2['ARM_ind'].fillna(0)
appr2['DTI updated'] = 0


##########################################################
#                       Code2Outcome
##########################################################

# def code2outcome():

# Call the function 'graph2code' from 'Graph2Code.py' to get the list of SQL queries
from Graph2Code import graph2code
sec_sheet, a, raw_attr = graph2code()
# print(raw_attr)

# Check for SQL query - raw attributes that are missing in appr2 dataset
count = 0
missing_attr = []
for item in raw_attr:
    if item in appr2.columns.to_list():
        count = count
    else:
        missing_attr.append(item)
        count = count + 1

if count == 0:

    # Run the SQL query and generate subrule outcomes
    from pandasql import sqldf
    mysql = lambda q: sqldf(q, globals()) # Standard Code


    for i in range(len(a)):
        query = a[len(a)-i-1].replace("\n","")
        data = mysql(query)


    data.to_csv("Testing_Section.csv", index = False)

else:
    print("The list of missing attributes in appr2 = {}".format(missing_attr))


# code2outcome()
