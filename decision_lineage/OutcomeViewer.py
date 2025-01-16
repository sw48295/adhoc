# Source: https://github.com/flatplanet/Intro-To-TKinter-Youtube-Course/blob/master/tree.py

# Import dependencies
from tkinter import *
from tkinter import ttk
import pandas as pd
import math
import numpy as np
import copy
import io
import os
import warnings
warnings.filterwarnings("ignore")

# Your current directory (change this)
# Directory = r"C:\Users\ShenliangWu\Downloads\DHL\Decision_Lineage"

# change directory
# os.chdir(Directory)

# type Id of the Loan number to be reviewed
Loan_num = 1010170116

# Read the data
data = pd.read_csv("Testing_Section.csv")

from Graph2Code import graph2code
section_sheet, a, raw_attr = graph2code()
df = pd.read_excel("Rule2Graph.xlsx", sheet_name= section_sheet)

# Remove all trailing or leading spaces in all columns of the dataframes
df[df.columns] = df.apply(lambda x: x.str.strip())
# data[data.columns] = data.apply(lambda x: x.str.strip())

# Convert column names to uppercase
df.columns = [x.upper() for x in df.columns]
df.NODE_ID = df.NODE_ID.str.upper()
df.NODE_DESC = df.NODE_DESC.str.upper()

# Modify ID columns
df['NODE_ID'] = df['NODE_ID'].str.replace("['.','-']","_")
df['PARENT_ID'] = df['PARENT_ID'].str.replace("['.','-']","_")

# Sort the data and add parent indices
df = df.sort_values(by=['PARENT_ID','NODE_ID'], ascending=[True, True])

# Cumulative count by group 'PARENT_DESC'
df['CUM_COUNT'] = df.groupby(['PARENT_ID']).cumcount()+1

df['VALUE'] = ''

data.columns =  [x.upper() for x in data.columns]

# (1) Remove OPERATOR if the rule/sub-rule is the first in the order
# (2) Rename attribute "NODE_ID"
for i in range(len(df)):
    df['OPERATOR'][i] = str(df['OPERATOR'][i]).upper()
    if df['CUM_COUNT'][i] == 1 or df['OPERATOR'][i]== 'NAN':
        df['OPERATOR'][i] = ""
    
    # Populates values of attributes
    if df['NODE_DESC'][i] in data.columns.tolist():
        df['VALUE'][i] = data[data["CLNT_LOAN_APLN_NBR"]==Loan_num][df['NODE_DESC'][i]].item()

    # Populates values of Rule_IDs
    if df['NODE_ID'][i] in data.columns.tolist():
        df['VALUE'][i] = data[data["CLNT_LOAN_APLN_NBR"]==Loan_num][df['NODE_ID'][i]].item()


# Create unique list of parents and their indices
Parent = df['NODE_ID'].to_list()
Parent = list(set(Parent))
Parent.sort()
PARENT_INDEX = [*range(1, len(Parent)+1, 1)]
df2 = pd.DataFrame(list(zip(Parent, PARENT_INDEX)), columns = ['id', 'idx'])
df2['index'] = "index" + df2['idx'].astype(str)

# Add PARENT_INDEX and node_index columns to main dataset
df = pd.merge(df, df2, how = 'left', left_on = ['PARENT_ID'], right_on = ['id'])
df = pd.merge(df, df2, how = 'left', left_on = ['NODE_ID'], right_on = ['id'])

#rename columns
df = df.rename(columns={'index_x':'PARENT_INDEX', 'index_y':'CHILD_INDEX', 'idx_y': 'INDEX_NUM'})

col_list = ['NODE_ID', 'PARENT_ID', 'NODE_TYPE', 'NODE_DESC', 'PARENT_DESC', 'VALUE', 'CUM_COUNT', 'OPERATOR', 'CHILD_INDEX', 'PARENT_INDEX', 'INDEX_NUM']
df = df[col_list]

df = df.drop_duplicates(subset=['NODE_ID', 'NODE_TYPE', 'NODE_DESC', 'PARENT_ID', 'PARENT_INDEX', 'CHILD_INDEX', 'INDEX_NUM'], keep="first")

df.to_csv("test_outcomeviewer.csv", index=False)


###############################################
            # Create Tkinter widget
###############################################
root = Tk()
root.title('DHL Decision Lineage - Hierarchial View')
# root.iconbitmap('c:/gui/codemy.ico')
root.geometry("1920x1080")

# Add some style
style = ttk.Style()
#Pick a theme
style.theme_use("default")
# Configure our treeview colors

style.configure("Treeview", 
	background="#D3D3D3",
	foreground="black",
	rowheight=25,
	fieldbackground="#D3D3D3"
	)
# Change selected color
style.map('Treeview', 
	background=[('selected', 'blue')])

# Create Treeview Frame
tree_frame = Frame(root)
tree_frame.pack(fill="both", expand=1)

# # Treeview Scrollbar
# tree_scroll = Scrollbar(tree_frame)
# tree_scroll.pack(side=RIGHT, fill=BOTH)

vscrollbar = ttk.Scrollbar(tree_frame, orient=VERTICAL)
vscrollbar.pack(side=RIGHT, fill=Y)

hscrollbar = ttk.Scrollbar(tree_frame, orient=HORIZONTAL)
hscrollbar.pack(side=BOTTOM, fill=X)


# Create Treeview
my_tree = ttk.Treeview(tree_frame, yscrollcommand=vscrollbar.set, xscrollcommand=hscrollbar.set, selectmode="extended")

# Pack to the screen
# my_tree.pack()
my_tree.pack(fill="both", expand=1)

# # Configure the scrollbar
# tree_scroll.config(command=my_tree.yview)

vscrollbar.configure(command=my_tree.yview)

hscrollbar.configure(command=my_tree.xview)

# Define Our Columns
my_tree['columns'] = ("Logic", "Value")

# Format Our Columns
my_tree.column("#0", anchor=W, width=1000, minwidth=25)
my_tree.column('Logic', anchor=W, width=50, minwidth=25)
# my_tree.column('Node', anchor=W, width=500, minwidth=25)
my_tree.column('Value', anchor=W, width=150, minwidth=25)

# Create Headings
my_tree.heading("#0", text="NODE", anchor=W) 
my_tree.heading('Logic', text='LOGIC', anchor=CENTER)
# my_tree.heading('Node', text='Node', anchor=W)
my_tree.heading('Value', text='VALUE', anchor=CENTER)


####### Inserting items to the my_tree ############

# Inserting parent
my_tree.insert(parent = "" , index = 1, iid = "index1", text = "Loan Number - " + str(Loan_num) + " : " + str(section_sheet) +  " Pass", values = ('',df[df.NODE_TYPE == 'master']['VALUE'].item()), tags= df[df.NODE_TYPE == 'master']['VALUE'].item())  #Sec 4 - Mortgage Statement Pass

# drop parent node from data
df.drop(df[df.NODE_TYPE == 'master'].index, inplace=True)

# Insert child branches
for index, row in df.iterrows():
    new_row = copy.deepcopy(row)
    new_dict= new_row.to_dict()

    if new_dict['NODE_TYPE'] == "raw_attr":
        tags_l = "ATTR"
        text_l = str(new_dict['OPERATOR']) + ' '+ str(new_dict['NODE_DESC'])
    else:
        tags_l = new_dict['VALUE']
        text_l = str(new_dict['OPERATOR']) + ' '+ str(new_dict['NODE_ID']) + ' ' + str(new_dict['NODE_DESC'])

    # Inserting branches
    my_tree.insert(parent = new_dict['PARENT_INDEX'] , index = new_dict['INDEX_NUM'], iid = new_dict['CHILD_INDEX'], text = text_l, values = (str(new_dict['OPERATOR']), str(new_dict['VALUE'])), tags= tags_l) 


# Color the 'PASS' tags green, 'FAIL' tags red, and 'ATTR' (atribute) tags orange
my_tree.tag_configure(1, background='light green') 
my_tree.tag_configure(0, background='salmon')
my_tree.tag_configure('ATTR', background='orange2')


root.mainloop()