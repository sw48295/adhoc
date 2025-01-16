# More simpler - direct way (reading all content)
# https://stackoverflow.com/questions/66374154/getting-the-list-numbers-of-list-items-in-docx-file-using-python-docx
#
# from asyncore import loop
from xml.dom.minidom import Element
import pandas as pd
import numpy as np
import os
import warnings
import re
import copy
warnings.filterwarnings("ignore")
#pip install python-docx

# Your current directory (change this)
# Directory = r"C:\Users\ShenliangWu\Downloads\DHL\Decision_Lineage"

# change directory
# os.chdir(Directory)

# Using docx2python
# file_path = r"C:\Users\ShenliangWu\Downloads\DHL\Decision_Lineage\6_Hazard_Insurance.docx"
# section_id = '6'

file_path = r"C:\Users\ShenliangWu\Downloads\DHL\Decision_Lineage\5_Tax_Insurance_Calculation.docx"
section_id = '5'


from docx2python import docx2python
from docx2python.iterators import iter_paragraphs

document = docx2python(file_path)

paragraphs = list(iter_paragraphs(document.document)) 
# print(paragraphs)


# Convert List to String
def listToString(s):   
    # initialize an empty string
    str1 = ""   
    # traverse in the string 
    for ele in s:
        if ele != '' and ele.count("\t") != 0:  # Remove blank or non-bullet lines
            str1 += ele.replace('\n', ' \ ')    # Keep seperate lines of same rule as one otherwise it leads to error when split later
            str1 += '\r'
        
    # return string 
    return str1       
       
f = open("tab_in.txt","w")
f.write(listToString(paragraphs))
f.close()


# Keep top Node
top_Node_ID = 'R_' + section_id
top_Node_Desc = paragraphs[0]
# print(top_Node_ID, top_Node_Desc, '\n\n')


# Generate All columns for Graph CSV
df1, df2, df3, df4, df5 = [], [], [], [], []

outfile = open("tab_out.txt", "w")
last_bullet_text=""

def find_nth(s, substr, n):
    x = -1
    for i in range(0, n):
        x = s.find(substr,x+1)
    return x

with open('tab_in.txt') as infile:
    for lineno, line in enumerate(infile):
        bullet_text = line.split(")\t",1)[0] + ')'
        desc_text = line.split(")\t",1)[1] 
        
        t = "\t"
        nt = bullet_text.count(t)
        t_len = len(t)
        
        if nt>0:
            old_parent = bullet_text[0:(nt-1)*t_len]
            new_parent_end = find_nth(last_bullet_text+t, t, nt)
            new_parent = last_bullet_text[0:new_parent_end]
            bullet_text = bullet_text.replace(old_parent,new_parent,1)

        outfile.write(bullet_text + ' ' + desc_text)

        Node_ID = 'R_' + section_id + '_' + bullet_text.replace('\t', '').replace(')', '_')
        Node_ID = Node_ID[0:len(Node_ID)-1] #Remove _ at end
        if int(Node_ID.split("_")[-1]) >= 10:
            Parent_ID = Node_ID[0:-3]
        else:
            Parent_ID = Node_ID[0:-2]

        # # Remove old ID
        # ID_index = desc_text.find('_')
        # if ID_index < 0:
        #     print('ID not in string ' + desc_text)
        # else:
        #     print(f'ID in string starting at index {ID_index} ')

        # if desc_text.startswith('_'):
        #     lst=desc_text.split(' ')
        #     lst.pop(0)
        #     var=' '.join(lst)
        #     var=var.strip()
        # print(lst)

        # Add Operator here
        if desc_text.split(' ')[0] == 'AND' or desc_text.split(' ')[0] == 'OR':
            Operator = desc_text.split(' ')[0]
            Node_Desc = desc_text.removeprefix(Operator + ' ').replace('\n', '')
        else:
            Operator = ''
            Node_Desc = desc_text.replace('\n', '')

        df1.append(Node_ID)
        df2.append(Node_Desc)
        df3.append(Parent_ID)
        df5.append(Operator)

        last_bullet_text = bullet_text
        
if outfile:
    outfile.close()

# Insert top Node to list
df1.insert(0, top_Node_ID)
df2.insert(0, top_Node_Desc)
df3.insert(0, '')
df5.insert(0, '')
# print(df1, df2, df3, df5)

# Look up node desc for Parent_ID
for k in range(0, len(df3)):
    for j in range (0, len(df1)-1):
        if df3[k] == df1[j]:
            Parent_Desc = df2[j] # Assign corresponding node desc to parent desc when matched
            break
        else:
            Parent_Desc = ''
    
    # print(df3[k], Parent_Desc)            
    df4.append(Parent_Desc)

# print(len(df1), len(df2), len(df3), len(df4), len(df5))

# Export Graph CSV file
rule_list = {'Node_ID': df1
             ,'Node_Desc': df2
             ,'Parent_ID': df3
             ,'Parent_Desc': df4
             ,'Operator': df5
            }

data = pd.DataFrame(rule_list, columns = ['Node_ID', 'Node_Desc', 'Parent_ID', 'Parent_Desc', 'Operator'])
data.to_csv("node_master.csv", index = False)
