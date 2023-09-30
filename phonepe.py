import streamlit as st
import pandas as pd
import json
import os
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text

from dotenv import load_dotenv

load_dotenv()

#import git

#github_url = "https://github.com/PhonePe/pulse.git"
#local_dir = "C:/Phonepe Pulse data"

#git.Repo.clone_from(github_url, local_dir)


# Data processing

agg_tran_path="C:/Phonepe Pulse data/data/aggregated/transaction/country/india/state"

agg_tran_state_list=os.listdir(agg_tran_path)

Agg_tra = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in agg_tran_state_list:
    path_state = agg_tran_path +"/"+i
    Agg_yr = os.listdir(path_state)

    for j in Agg_yr:
        path_yr = path_state + "/"+ j
        Agg_yr_list = os.listdir(path_yr)

        for k in Agg_yr_list:
            path_yr_list = path_yr + "/"+ k
            Data = open(path_yr_list, 'r')
            A = json.load(Data)

            for l in A['data']['transactionData']:
                Name = l['name']
                count = l['paymentInstruments'][0]['count']
                amount = l['paymentInstruments'][0]['amount']
                Agg_tra['State'].append(i)
                Agg_tra['Year'].append(j)
                Agg_tra['Quarter'].append(int(k.strip('.json')))
                Agg_tra['Transaction_type'].append(Name)
                Agg_tra['Transaction_count'].append(count)
                Agg_tra['Transaction_amount'].append(amount)

df_aggregated_transaction = pd.DataFrame(Agg_tra)

#aggregated user

agg_user_path= "C:/Phonepe Pulse data/data/aggregated/user/country/india/state"

agg_user_state_list=os.listdir(agg_user_path)

Agg_user = {'State': [], 'Year': [], 'Quarter': [], 'Brand': [], 'User_Count': [],'User_Percentage':[]}

for i in agg_user_state_list:
    path_state = agg_user_path +"/"+i
    Agg_yr = os.listdir(path_state)

    for j in Agg_yr:
        path_year = path_state + "/"+ j
        Agg_yr_list = os.listdir(path_year)

        for k in Agg_yr_list:
            path_yr_list = path_year + "/"+ k
            agg_user_data = open(path_yr_list, 'r')
            B = json.load(agg_user_data)

            try:
                for l in B['data']["usersByDevice"]:
                        brand= l['brand']
                        Count = l['count']
                        Percentage = l['percentage']
                        Agg_user['State'].append(i)
                        Agg_user['Year'].append(j)
                        Agg_user['Quarter'].append(int(k.strip('.json')))
                        Agg_user['Brand'].append(brand)
                        Agg_user['User_Count'].append(Count)
                        Agg_user['User_Percentage'].append(Percentage)
            except:
                pass


df_aggregated_user = pd.DataFrame(Agg_user)

# Map transaction

map_tran_path="C:/Phonepe Pulse data/data/map/transaction/hover/country/india/state"

map_tran_states_list=os.listdir(map_tran_path)

map_tran = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'District_Tran_Count': [],'District_Tran_Amount':[]}

for i in map_tran_states_list:
    path_state = map_tran_path +"/"+i
    map_yr = os.listdir(path_state)

    for j in map_yr:
        path_yr = path_state + "/"+ j
        map_yr_list = os.listdir(path_yr)

        for k in map_yr_list:
            path_yr_list = path_yr + "/"+ k
            map_tran_data = open(path_yr_list, 'r')
            C = json.load(map_tran_data)

            for l in C['data']['hoverDataList']:
                        district= l['name']
                        count = l['metric'][0]['count']
                        amount = l['metric'][0]['amount']
                        map_tran['State'].append(i)
                        map_tran['Year'].append(j)
                        map_tran['Quarter'].append(int(k.strip('.json')))
                        map_tran['District'].append(district)
                        map_tran['District_Tran_Count'].append(count)
                        map_tran['District_Tran_Amount'].append(amount)



df_map_transaction = pd.DataFrame(map_tran)

# Map user

map_user_path="C:/Phonepe Pulse data/data/map/user/hover/country/india/state"

map_user_states_list=os.listdir(map_user_path)

map_user = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'District_Users': [],'District_Appopens':[]}

for i in map_user_states_list:
    path_state = map_user_path +"/"+i
    map_yr = os.listdir(path_state)

    for j in map_yr:
        path_yr = path_state + "/"+ j
        map_yr_list = os.listdir(path_yr)

        for k in map_yr_list:
            path_yr_list = path_yr + "/"+ k
            map_user_data = open(path_yr_list, 'r')
            D = json.load(map_user_data)

            for l in D['data']['hoverData'].items():
                    district= l[0]
                    user = l[1]['registeredUsers']
                    appopens = l[1]['appOpens']
                    map_user['State'].append(i)
                    map_user['Year'].append(j)
                    map_user['Quarter'].append(int(k.strip('.json')))
                    map_user['District'].append(district)
                    map_user['District_Users'].append(user)
                    map_user['District_Appopens'].append(appopens)



df_map_user = pd.DataFrame(map_user)

# Top transaction of districts

top_tran_path="C:/Phonepe Pulse data/data/top/transaction/country/india/state"

top_tran_states_list=os.listdir(top_tran_path)

top_tran = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'District_trans_count': [],'District_Amount':[]}

for i in top_tran_states_list:
    path_state = top_tran_path +"/"+i
    top_yr = os.listdir(path_state)

    for j in top_yr:
        path_yr = path_state + "/"+ j
        top_yr_list = os.listdir(path_yr)

        for k in top_yr_list:
            path_yr_list = path_yr + "/"+ k
            top_tran_data = open(path_yr_list, 'r')
            E = json.load(top_tran_data)

            for l in E['data']['districts']:
                    district= l['entityName']
                    counts = l['metric']['count']
                    amounts = l['metric']['amount']
                    top_tran['State'].append(i)
                    top_tran['Year'].append(j)
                    top_tran['Quarter'].append(int(k.strip('.json')))
                    top_tran['District'].append(district)
                    top_tran['District_trans_count'].append(counts)
                    top_tran['District_Amount'].append(amounts)



df_Top_transaction_district = pd.DataFrame(top_tran)

# Top Transaction with pincode

top_tran_states_list=os.listdir(top_tran_path)

top_tran_pincode = {'State': [], 'Year': [], 'Quarter': [], 'Pincode_trans': [], 'Transaction_count': [],'Transaction_amount':[]}

for i in top_tran_states_list:
    path_state = top_tran_path +"/"+i
    top_yr = os.listdir(path_state)

    for j in top_yr:
        path_yr = path_state + "/"+ j
        top_yr_list = os.listdir(path_yr)

        for k in top_yr_list:
            path_yr_list = path_yr + "/"+ k
            top_tran_data = open(path_yr_list, 'r')
            F = json.load(top_tran_data)

            for l in F['data']['pincodes']:
                    district= l['entityName']
                    counts = l['metric']['count']
                    amounts = l['metric']['amount']
                    top_tran_pincode['State'].append(i)
                    top_tran_pincode['Year'].append(j)
                    top_tran_pincode['Quarter'].append(int(k.strip('.json')))
                    top_tran_pincode['Pincode_trans'].append(district)
                    top_tran_pincode['Transaction_count'].append(counts)
                    top_tran_pincode['Transaction_amount'].append(amounts)



df_top_transaction = pd.DataFrame(top_tran_pincode)

# Top user of districts

top_user_path="C:/Phonepe Pulse data/data/top/user/country/india/state"

top_user_states_list=os.listdir(top_user_path)

top_user = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'District_Users': []}

for i in top_user_states_list:
    path_state = top_user_path +"/"+i
    top_yr = os.listdir(path_state)

    for j in top_yr:
        path_yr = path_state + "/"+ j
        top_yr_list = os.listdir(path_yr)

        for k in top_yr_list:
            path_yr_list = path_yr + "/"+ k
            top_user_data = open(path_yr_list, 'r')
            G = json.load(top_user_data)

            for l in G['data']['districts']:
                        district= l['name']
                        user = l['registeredUsers']

                        top_user['State'].append(i)
                        top_user['Year'].append(j)
                        top_user['Quarter'].append(int(k.strip('.json')))
                        top_user['District'].append(district)
                        top_user['District_Users'].append(user)




df_top_user_district = pd.DataFrame(top_user)


# Top user with pincodes

top_user_states_list=os.listdir(top_user_path)

top_user_pincode = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Registered_Users': []}

for i in top_user_states_list:
    path_state = top_user_path +"/"+i
    top_yr = os.listdir(path_state)

    for j in top_yr:
        path_yr = path_state + "/"+ j
        top_yr_list = os.listdir(path_yr)

        for k in top_yr_list:
            path_yr_list = path_yr + "/"+ k
            top_user_data = open(path_yr_list, 'r')
            H = json.load(top_user_data)

            for l in H['data']['pincodes']:
                        pincodes= l['name']
                        user = l['registeredUsers']
                        top_user_pincode['State'].append(i)
                        top_user_pincode['Year'].append(j)
                        top_user_pincode['Quarter'].append(int(k.strip('.json')))
                        top_user_pincode['Pincode'].append(pincodes)
                        top_user_pincode['Registered_Users'].append(user)




df_top_user = pd.DataFrame(top_user_pincode)

# connect to mysql server

mydb = mysql.connector.connect(
  host = "localhost",
  user = "root",
  port= 3306,
  password = os.getenv("MYSQLKEY"),
  database="Phonepe"
)

mycursor = mydb.cursor()



# Connect to the new created database
engine = create_engine(os.getenv("MYSQLSRC"), echo=False)


# 1
df_aggregated_transaction.to_sql('aggregated_transaction', engine, if_exists = 'replace', index=False,   
                                 dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                       'Year': sqlalchemy.types.Integer, 
                                       'Quater': sqlalchemy.types.Integer, 
                                       'Transaction_type': sqlalchemy.types.VARCHAR(length=50), 
                                       'Transaction_count': sqlalchemy.types.Integer,
                                       'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
# 2
df_aggregated_user.to_sql('aggregated_user', engine, if_exists = 'replace', index=False,
                          dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                 'Year': sqlalchemy.types.Integer, 
                                 'Quater': sqlalchemy.types.Integer,
                                 'Brands': sqlalchemy.types.VARCHAR(length=50), 
                                 'User_Count': sqlalchemy.types.Integer, 
                                 'User_Percentage': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
# 3                       
df_map_transaction.to_sql('map_transaction', engine, if_exists = 'replace', index=False,
                          dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                 'Year': sqlalchemy.types.Integer, 
                                 'Quater': sqlalchemy.types.Integer, 
                                 'District': sqlalchemy.types.VARCHAR(length=50), 
                                 'Transaction_Count': sqlalchemy.types.Integer, 
                                 'Transaction_Amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
# 4
df_map_user.to_sql('map_user', engine, if_exists = 'replace', index=False,
                   dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                          'Year': sqlalchemy.types.Integer, 
                          'Quater': sqlalchemy.types.Integer, 
                          'District': sqlalchemy.types.VARCHAR(length=50), 
                          'Registered_User': sqlalchemy.types.Integer })



# 5 
df_Top_transaction_district.to_sql('top_transaction_district', engine, if_exists = 'replace', index=False,
                         dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                'Year': sqlalchemy.types.Integer, 
                                'Quater': sqlalchemy.types.Integer,   
                                'District': sqlalchemy.types.VARCHAR(length=100),
                                'Transaction_count': sqlalchemy.types.Integer, 
                                'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
                                                                             
#6
df_top_transaction.to_sql('top_transaction', engine, if_exists = 'replace', index=False,
                         dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                'Year': sqlalchemy.types.Integer, 
                                'Quater': sqlalchemy.types.Integer,   
                                'District_Pincode': sqlalchemy.types.Integer,
                                'Transaction_count': sqlalchemy.types.Integer, 
                                'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

#7
df_top_user_district.to_sql('top_user_district', engine, if_exists = 'replace', index=False,
                   dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                          'Year': sqlalchemy.types.Integer, 
                          'Quater': sqlalchemy.types.Integer,                           
                          'District': sqlalchemy.types.VARCHAR(length=100), 
                          'Districts_Registered_User': sqlalchemy.types.Integer,})

#8
df_top_user.to_sql('top_user', engine, if_exists = 'replace', index=False,
                   dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                          'Year': sqlalchemy.types.Integer, 
                          'Quater': sqlalchemy.types.Integer,                           
                          'District_Pincode': sqlalchemy.types.Integer, 
                          'Registered_User': sqlalchemy.types.Integer,})




