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
# Directory = r"C:\Users\ShenliangWu\Downloads\DHL\Decision_Lineage"

# change directory
# os.chdir(Directory)


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


# Sec-5
appr2['has_escrow_for_all_1st_n_2nd_lien_NY_subprime_loans'] = 'Yes'
appr2['tax_ins_in_DTI_calc_if_NY_1st_lien_subprime'] = 'Yes'
appr2['tax_need_to_be_paid_at_closing'] = 'Yes'
appr2['property_in_TX'] = 'No'
appr2['has_dlq_tax_but_proof_pymt_provided'] = 'Yes'
appr2['borrower_correct_on_tax_cert'] = 'Yes'
appr2['tax_cert_show_multi_parcels_taxed'] = 'No'
appr2['paid_date_freq_tax_parcel_ID_available_valid'] = 'Yes'
appr2['tax_cert_show_property_type'] = 'Yes'
appr2['tax_noted_on_ms_if_DHL_2nd_lien'] = 'Yes'
appr2['doc_not_modified'] = 'Yes'
appr2['property_addr_correct_on_tax_cert'] = 'Yes'
appr2['multiple_tax_cert_not_submitted'] = 'Yes'
appr2['parcel_num_correct_on_tax_cert'] = 'Yes'
appr2['check_property_tax_changed_in_county_records'] = 'Yes'
appr2['tax_counted_in_DTI_calc'] = 'Yes'
appr2['property_in_NY'] = 'No'
appr2['supplemental_tax_paid_in_full_if_due_45d'] = 'Yes'
appr2['has_escrow_for_tax_ins_on_min_5y_1st_lien_HPML_n_NY_subprime_loans'] = 'Yes'
appr2['tax_data_entered_NetO_correctly'] = 'Yes'
appr2['county_correct_on_tax_cert'] = 'Yes'
appr2['has_rcnt_transfer_of_ownership_6m'] = 'Yes'
appr2['tax_cert_dated'] = 'Yes'
appr2['dlq_tax_is_goodthrough_date_on_tax_cert'] = 'Yes'
appr2['tax_cert_tax_match_ms'] = 'Yes'
appr2['HOA_maintains_master_policy_if_property_type_condo'] = 'Yes'
appr2['no_tax_dlq_in_tax_cert_or_title_or_ms'] = 'Yes'
appr2['escrow_tax_ins_due_in_45d_after_final_approval_n_paid_if_1st_lien_loan'] = 'Yes'
appr2['tax_calc_escrow'] = 'Yes'
appr2['tax_cert_in_1st_lien'] = 'Yes'
appr2['doc_has_multiple_parcel_IDs'] = 'No'
appr2['client_correct_on_tax_cert'] = 'Yes'
appr2['tax_contacts_complete_w_tax_auth_addr_phone_num'] = 'Yes'
appr2['tax_noted_on_tax_cert'] = 'Yes'
appr2['has_2m_cushion_where_escrow_required'] = 'Yes'
appr2['tax_cert_tax_match_title_report'] = 'Yes'


# Sec-6
appr2['property_covered'] = 1
appr2['premium_paid_by_or_due_for_DTI_calc'] = 1
appr2['voluntary_ins_not_included_in_DTI_calc'] = 1
appr2['escrowed_in_existing_first_mtg'] = 1
appr2['ins_cost_included_in_DTI_if_optional_undetermined'] = 1
appr2['doc_w_HOI_ins_premium_obtained_from_ins_agent'] = 1
appr2['total_premium_included_in_DTI_calc'] = 1
appr2['Min_one_Borrower_name_listed'] = 1
appr2['expiraion_date_policy'] = '2022-09-01'
appr2['final_approval_date'] = '2022-07-01'
appr2['coverage_not_increased'] = 1
appr2['PRPRTY_ST_in_windstorm_prone_st'] = 0
appr2['windstorm_required_doc_not_expired'] = 1
appr2['correct_property_for_windstorm_check'] = 1
appr2['mailing_addr_on_ins_match_1003'] = 1
appr2['Mailing_address_PO_box_check'] = 1
appr2['insurance_company_name_match'] = 1                                                                                
appr2['master_condo_policy_included'] = 1
appr2['HO6_coverage_ge_20pct_appraised_value'] = 1
appr2['policy_number_visibility_check'] = 1
appr2['does_policy_indicate_any_relationship'] = 0
appr2['loan_in_NY_SubPrime'] = 0
appr2['premium_listed_on_policy'] = 1
appr2['ineligible_home_type_in_ins_doc'] = 0
appr2['privately_held_check'] = 0
appr2['small_business_check'] = 0
appr2['policy_start_and_end_date_present'] = 1                                        
appr2['application_date'] = '2022-06-11'
appr2['coverage_amount_within_policy_for_requested_amt'] = 1
appr2['open_PTP_or_PTA'] = 0
appr2['all_insurance_types_included_in_DTI'] = 1
appr2['additional_properties_listed'] = 0
appr2['hoi_statement_indication_for_due_or_past_due'] = 0
appr2['only_one_hoi_included'] = 1
appr2['more_than_one_property'] = 0
appr2['Doc_modified'] = 0
appr2['is_dhl_listed_as_mortgage_or_interested_party'] = 0                
appr2['current_insurance_escrowed'] = 1
appr2['supplement_included_in_the_current_escrow'] = 0


# Sec-8
appr2['All_pages_of_BS'] = 1
appr2['BS_not_expired_or_latest'] = 1
appr2['two_months_BS'] = 1
appr2['unusual_deposit_activity'] = 1
appr2['BS_is_valid_or_invalid'] = 1
appr2['Liquid_Asset_updated'] = 1
appr2['Liability_updated'] = 1
appr2['Address_on_BS_matches_1003'] = 1
appr2['Non_Borrower'] = 1
appr2['account_balance_adjustment'] = 1
appr2['LOE_required'] = 1
appr2['BS_used_to_validate_income'] = 1
appr2['assets_statements_for_income'] = 1
appr2['BS_used_to_OMIT_liability'] = 1
appr2['check_for_cancelled_checks'] = 1
appr2['requirements_for_income_section'] = 1
appr2['Seasoned_Funds'] = 1
appr2['big_deposits'] = 1


# Sec-13
appr2['disclosures_sent_on_time'] = 1
appr2['disclosures_sent_PTA'] = 1
appr2['doc_validation_elevation_survey'] = 1
appr2['doc_validation_map_revision_letter_of_map_amendment'] = 1
appr2['flood_zone_conditions_triggered'] = 1
appr2['Flood_Notices_sent'] = 1
appr2['flood_insurance_available'] = 1
appr2['flood_cert_correct'] = 1
appr2['borrowers_and_property_address_correct'] = 1
appr2['loan_number_correct'] = 1
appr2['NFIP_community_name_correct'] = 1
appr2['County_info_correct'] = 1
appr2['state_info_correct'] = 1
appr2['NFIP_Community_Number'] = 1
appr2['NFIP_Map_and_Panel_Number'] = 1
appr2['date_entry_populated'] = 1
appr2['sec2_subsecC_boxes_checked'] = 1
appr2['sec2_subsecD_boxes_checked'] = 1
appr2['Address_flood_cert'] = 'ABC'
appr2['address_on_application'] = 'ABC'
appr2['flood_cert_full_loan_life'] = 1
appr2['Community_Information_Flood_Determination_tab'] = 'ABC'
appr2['Community_Information_Flood_Certification_tab'] = 'ABC'
appr2['Community_NFIP_Ind'] = 1
appr2['Property_Information_Flood_Determination_tab'] = 'ABC'
appr2['Property_Information_Flood_Certification_tab'] = 'ABC'
appr2['Certification_Information_Flood_Determination_tab'] = 'ABC'
appr2['Certification_Information_Flood_Certification_tab'] = 'ABC'
appr2['Status_Information_Flood_Determination_tab'] = 'ABC'
appr2['Status_Information_Current_Flood_Certification_request'] = 'ABC'
appr2['flood_cert_date'] =  '2022-09-16'
appr2['closing_date'] =  '2022-10-16'


# Sec-15
appr2['Review_Loan_Summary_Tab'] = 1
# appr2['Application_Date'] = '2022-06-11'
appr2['first_UW_date'] = '2022-02-16'    
appr2['Review_File_Back_In_Pre'] = 1
appr2['Review_Loan_Not_ATP_ECOA_Violation'] = 1
appr2['Review_For_Resubmit_Validate_ECOA'] = 1
appr2['Review_For_Resubmit'] = 1
appr2['FICO_Score_Populated_For_All_Parties'] = 1
appr2['NBS_DP_CU_RB_On_Application'] = 1
appr2['Review_Documents_Need_To_Be_Conditioned_Prior_To_UW'] = 1
appr2['Review_File_Is_Ready_For_UW'] = 1
appr2['All_Comments_Reviewed'] = 1
appr2['How_Property_Was_Acquired_qualified'] = 1
appr2['Documentation_On_Acquisition_Of_Property'] = 1
appr2['required_sections_completed'] = 'Yes'
appr2['complete_2y_hist_provided_if_applicant_w_curr_employer_lt_2y'] = 'Yes'
appr2['Application_1003_Is_Correct'] = 'Yes'
# appr2['VVOE_Is_Complete'] = 'Yes'
# appr2['VVOE_Is_Correct'] = 'Yes'

# Sec-16
appr2['NBS_US_citizen'] = 1
appr2['NBS_ID_provided'] = 1
appr2['NBS_passport_provided'] = 1
appr2['LOE_for_Photo_ID_address_mismatch'] = 1
appr2['DOB_on_Photo_ID'] = 1
appr2['DOB_on_1003'] = 1
appr2['LOE_for_Photo_ID_DOB_mismatch'] = 1
appr2['Name_on_Photo_ID'] = 'ABC'
appr2['Name_on_1003'] = 'ABC'
appr2['LOE_for_Photo_ID_Name_mismatch'] = 1
appr2['Address_Photo_ID'] = 'ABC'
appr2['Address_1003'] = 'ABC'
appr2['LOE_for_Photo_ID_address_mismatch'] = 1
appr2['Third_Party_US_citizen'] = 1
appr2['Third_Party_ID_provided'] = 1
appr2['Third_Party_passport_provided'] = 1
appr2['Category_DL_State_ID_LDM_screen'] = 'X'
appr2['Category_DL_State_ID_netoxygen'] = 'X'
appr2['Type_DL_State_ID_LDM_screen'] = 'X'
appr2['Type_DL_State_ID_netoxygen'] = 'X'
appr2['BO_NOT_married'] = 1
appr2['NO_third_party_on_title'] = 1
appr2['is_borrower_citizen'] = 1
appr2['is_borrower_pr'] = 1
appr2['BO_phy_mental_impaired'] = 1
appr2['VA_guidelines_for_abuse_met'] = 1
appr2['BO_60_plus_years_old'] = 1
appr2['VA_guidelines_for_old_age_met'] = 1
appr2['ID_NOT_expired'] = 1
appr2['BO_Nationality_CUBA'] = 1
appr2['Resident_alien_card_provided'] = 1
appr2['state_on_State_ID'] = 'XYZ'
appr2['state_on_1003'] = 'XYZ'
appr2['ID_requirements_met_manually'] = 1
appr2['Photo_ID'] = 1




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