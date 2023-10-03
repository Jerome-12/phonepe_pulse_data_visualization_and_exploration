
# Import libraries

import requests
import subprocess

import pandas as pd
import numpy as np
import json


import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine


import streamlit as st
import plotly.express as px
import os
from dotenv import load_dotenv

load_dotenv()

# DASHBOARD 

# Connect to SQL 

cont = mysql.connector.connect(host='localhost', user='root', password=os.getenv("MYSQLKEY"), db='phonepe')
cursor = cont.cursor()

# Streamlit page 

# Configuring Streamlit GUI 
st.set_page_config(page_title='Phonepe Pulse',page_icon='bar_chart',layout='wide')

# Title
st.header(':violet[Phonepe Pulse Data Visualization and Exploration ]')

# Selection option
option = st.radio('**Select the option**',('Overall India', 'State wise','Top ten State - year wise','Top tens'),horizontal=True)

# Overall India    

if option == 'Overall India':

    # Select tab
    tab1, tab2 = st.tabs(['Transaction','User'])

    # Overall India Transaction   
    with tab1:
        col1, col2, col3 = st.columns(3)
        with col1:
            in_tr_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='in_tr_yr')
        with col2:
            in_tr_qtr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='in_tr_qtr')
        with col3:
            in_tr_tr_typ = st.selectbox('**Select Transaction type**', ('Recharge & bill payments','Peer-to-peer payments',
            'Merchant payments','Financial Services','Others'),key='in_tr_tr_typ')

        # SQL Query

        # Transaction Analysis bar chart query
        try:
            cursor.execute(f"SELECT State, Transaction_amount FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
            Trans_tab = cursor.fetchall()
            df_Trans_tab = pd.DataFrame(np.array(Trans_tab), columns=['State', 'Transaction_amount'])
            df_df_Trans_tab = df_Trans_tab.set_index(pd.Index(range(1, len(df_Trans_tab)+1)))

            # Transaction Analysis table query
            cursor.execute(f"SELECT State, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
            Trans_alys = cursor.fetchall()
            df_Trans_alys = pd.DataFrame(np.array(Trans_alys), columns=['State','Transaction_count','Transaction_amount'])
            df_df_Trans_alys = df_Trans_alys.set_index(pd.Index(range(1, len(df_Trans_alys)+1)))

            # Total Transaction Amount table query
            cursor.execute(f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
            Trans_amt = cursor.fetchall()
            df_Trans_amt = pd.DataFrame(np.array(Trans_amt), columns=['Total','Average'])
            df_df_Trans_amt = df_Trans_amt.set_index(['Average'])
            
            # Total Transaction Count table query
            cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
            Trans_cnt = cursor.fetchall()
            df_Trans_cnt = pd.DataFrame(np.array(Trans_cnt), columns=['Total','Average'])
            df_df_Trans_cnt  = df_Trans_cnt.set_index(['Average'])

            # Output 

            # Geo visualization dashboard for Transaction 
            # Drop a State column from df_Trans_tab
            df_Trans_tab.drop(columns=['State'], inplace=True)
            # Clone the gio data
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data1 = json.loads(response.content)
            # Extract state names and sort them in alphabetical order
            state_names_trans = [feature['properties']['ST_NM'] for feature in data1['features']]
            state_names_trans.sort()
            # Create a DataFrame with the state names column
            df_state_names_trans = pd.DataFrame({'State': state_names_trans})
            # Combine the Gio State name with df_Trans_tab
            df_state_names_trans['Transaction_amount']=df_Trans_tab
            # convert dataframe to csv file
            df_state_names_trans.to_csv('State_trans.csv', index=False)
            # Read csv
            df_trans = pd.read_csv('State_trans.csv')
            # Geo plot
            fig_trans = px.choropleth(
                df_trans,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',locations='State',color='Transaction_amount',color_continuous_scale='thermal',title = 'Transaction Analysis')
            fig_trans.update_geos(fitbounds="locations", visible=False)
            fig_trans.update_layout(title_font=dict(size=33),title_font_color='#6739b7', height=800)
            st.plotly_chart(fig_trans,use_container_width=True)

            # Overall India Transaction Analysis Bar chart 
            df_df_Trans_tab['State'] = df_df_Trans_tab['State'].astype(str)
            df_df_Trans_tab['Transaction_amount'] = df_df_Trans_tab['Transaction_amount'].astype(float)
            df_df_Trans_tab_fig = px.bar(df_df_Trans_tab , x = 'State', y ='Transaction_amount', color ='Transaction_amount', color_continuous_scale = 'thermal', title = 'Transaction Analysis Chart', height = 700,)
            df_df_Trans_tab_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
            st.plotly_chart(df_df_Trans_tab_fig,use_container_width=True)

            # Overall India Total Transaction calculation Table
            st.header(':violet[Total calculation]')

            col4, col5 = st.columns(2)
            with col4:
                st.subheader('Transaction Analysis')
                st.dataframe(df_df_Trans_alys)
            with col5:
                st.subheader('Transaction Amount')
                st.dataframe(df_df_Trans_amt)
                st.subheader('Transaction Count')
                st.dataframe(df_df_Trans_cnt)
        except ValueError:
            st.write(':red[Data not available]')

    # Overall India User 
    with tab2:
        
        col1, col2 = st.columns(2)
        with col1:
            in_us_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='in_us_yr')
        with col2:
            in_us_qtr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='in_us_qtr')
        
        # SQL Query

        # User Analysis Bar chart query
        try:
            cursor.execute(f"SELECT State, SUM(User_Count) FROM aggregated_user WHERE Year = '{in_us_yr}' AND Quarter = '{in_us_qtr}' GROUP BY State;")
            user_tab = cursor.fetchall()
            df_user_tab = pd.DataFrame(np.array(user_tab), columns=['State', 'User Count'])
            df_df_user_tab = df_user_tab.set_index(pd.Index(range(1, len(df_user_tab)+1)))

            # Total User Count table query
            cursor.execute(f"SELECT SUM(User_Count), AVG(User_Count) FROM aggregated_user WHERE Year = '{in_us_yr}' AND Quarter = '{in_us_qtr}';")
            user_cnt = cursor.fetchall()
            df_user_cnt = pd.DataFrame(np.array(user_cnt), columns=['Total','Average'])
            df_df_user_cnt = df_user_cnt.set_index(['Average'])

            # Output

            # Geo visualization dashboard for User 

            # Drop a State column from df_user_tab
            df_user_tab.drop(columns=['State'], inplace=True)
            # Clone the gio data
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data2 = json.loads(response.content)
            # Extract state names and sort them in alphabetical order
            state_names_use = [feature['properties']['ST_NM'] for feature in data2['features']]
            state_names_use.sort()
            # Create a DataFrame with the state names column
            df_state_names_use = pd.DataFrame({'State': state_names_use})
            # Combine the Geo State name with df_Trans_tab
            df_state_names_use['User Count']=df_user_tab
            # convert dataframe to csv file
            df_state_names_use.to_csv('State_user.csv', index=False)
            # Read csv
            df_use = pd.read_csv('State_user.csv')
            # Geo plot
            fig_use = px.choropleth(
                df_use,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',locations='State',color='User Count',color_continuous_scale='thermal',title = 'User Analysis')
            fig_use.update_geos(fitbounds="locations", visible=False)
            fig_use.update_layout(title_font=dict(size=33),title_font_color='#6739b7', height=800)
            st.plotly_chart(fig_use,use_container_width=True)

            # Overall India User Analysis Bar chart  
        
            df_df_user_tab['State'] = df_df_user_tab['State'].astype(str)
            df_df_user_tab['User Count'] = df_df_user_tab['User Count'].astype(int)
            df_df_user_tab_fig = px.bar(df_df_user_tab , x = 'State', y ='User Count', color ='User Count', color_continuous_scale = 'thermal', title = 'User Analysis Chart', height = 700,)
            df_df_user_tab_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
            st.plotly_chart(df_df_user_tab_fig,use_container_width=True)

            # Overall India Total User calculation Table
            st.header(':violet[Total calculation]')

            col3, col4 = st.columns(2)
            with col3:
                st.subheader('User Analysis')
                st.dataframe(df_df_user_tab)
            with col4:
                st.subheader('User Count')
                st.dataframe(df_df_user_cnt)
        except ValueError:
            st.write(':red[Data not available]')        


# State wise

elif option =='State wise':

    # Select tab
    tab3, tab4 = st.tabs(['Transaction','User'])

    # State wise Transaction 
    with tab3:

        col1, col2,col3 = st.columns(3)
        with col1:
            st_tr_st = st.selectbox('**Select State**',('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh','assam', 'bihar', 
            'chandigarh', 'chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 
            'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh','maharashtra', 'manipur', 
            'meghalaya', 'mizoram', 'nagaland','odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim', 'tamil-nadu', 'telangana', 
            'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'),key='st_tr_st')
        with col2:
            st_tr_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='st_tr_yr')
        with col3:
            st_tr_qtr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='st_tr_qtr')
        
        # SQL Query

        # Transaction Analysis bar chart query
        try:
            cursor.execute(f"SELECT Transaction_type, Transaction_amount FROM aggregated_transaction WHERE State = '{st_tr_st}' AND Year = '{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_Trans_bar = cursor.fetchall()
            df_st_Trans_bar = pd.DataFrame(np.array(st_Trans_bar), columns=['Transaction_type', 'Transaction_amount'])
            df_df_st_Trans_bar = df_st_Trans_bar.set_index(pd.Index(range(1, len(df_st_Trans_bar)+1)))

            # Transaction Analysis table query
            cursor.execute(f"SELECT Transaction_type, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE State = '{st_tr_st}' AND Year = '{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_Trans_alys = cursor.fetchall()
            df_st_Trans_alys = pd.DataFrame(np.array(st_Trans_alys), columns=['Transaction_type','Transaction_count','Transaction_amount'])
            df_df_st_Trans_alys = df_st_Trans_alys.set_index(pd.Index(range(1, len(df_st_Trans_alys)+1)))

            # Total Transaction Amount table query
            cursor.execute(f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_transaction WHERE State = '{st_tr_st}' AND Year = '{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_Trans_amt = cursor.fetchall()
            df_st_Trans_amt = pd.DataFrame(np.array(st_Trans_amt), columns=['Total','Average'])
            df_df_st_Trans_amt = df_st_Trans_amt.set_index(['Average'])
            
            # Total Transaction Count table query
            cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE State = '{st_tr_st}' AND Year ='{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_Trans_cnt = cursor.fetchall()
            df_st_Trans_cnt = pd.DataFrame(np.array(st_Trans_cnt), columns=['Total','Average'])
            df_df_st_Trans_cnt = df_st_Trans_cnt.set_index(['Average'])

            # Output 

            # State wise Transaction Analysis bar chart 
            df_df_st_Trans_bar['Transaction_type'] = df_df_st_Trans_bar['Transaction_type'].astype(str)
            df_df_st_Trans_bar['Transaction_amount'] = df_df_st_Trans_bar['Transaction_amount'].astype(float)

            # bar chart

            df_df_st_Trans_bar_fig = px.bar(df_df_st_Trans_bar , x = 'Transaction_type', y ='Transaction_amount', color ='Transaction_amount', color_continuous_scale = 'thermal', title = 'Transaction Analysis Chart', height = 500,)
            df_df_st_Trans_bar_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
            st.plotly_chart(df_df_st_Trans_bar_fig,use_container_width=True)

            #pie chart

            #df_df_st_Trans_bar_fig = px.pie(df_df_st_Trans_bar , names = 'Transaction_type', values ='Transaction_amount',hole=0.4) 
            #st.plotly_chart(df_df_st_Trans_bar_fig)


            # State wise Total Transaction calculation Table
            st.header(':violet[Total calculation]')

            col4, col5 = st.columns(2)
            with col4:
                st.subheader('Transaction Analysis')
                st.dataframe(df_df_st_Trans_alys)
            with col5:
                st.subheader('Transaction Amount')
                st.dataframe(df_df_st_Trans_amt)
                st.subheader('Transaction Count')
                st.dataframe(df_df_st_Trans_cnt)
        except ValueError:
            st.write(':red[Data not available]')       


    # Statewise Users 
    with tab4:
        
        col5, col6,col7 = st.columns(3)
        with col5:
            st_us_st = st.selectbox('**Select State**',('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh','assam', 'bihar', 
            'chandigarh', 'chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 
            'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh','maharashtra', 'manipur', 
            'meghalaya', 'mizoram', 'nagaland','odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim', 'tamil-nadu', 'telangana', 
            'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'),key='st_us_st')
        with col6:
            st_us_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='st_us_yr')
        with col7:
             st_us_qtr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='st_us_qtr') 

        # SQL Query

        # User Analysis Bar chart query
        try:
            cursor.execute(f"SELECT Brand, User_Count FROM aggregated_user WHERE State ='{st_us_st}' AND Year = '{st_us_yr}'AND Quarter='{st_us_qtr}';")
            st_user_tab = cursor.fetchall()
            df_st_user_tab = pd.DataFrame(np.array(st_user_tab), columns=['Brands', 'User Count'])
            df_df_st_user_tab = df_st_user_tab.set_index(pd.Index(range(1, len(df_st_user_tab)+1)))
        
        
            # Total User Count table query
            cursor.execute(f"SELECT SUM(User_Count), AVG(User_Count) FROM aggregated_user WHERE State = '{st_us_st}' AND Year = '{st_us_yr}'AND Quarter='{st_us_qtr}';")
            st_user_cnt = cursor.fetchall()
            df_st_user_cnt = pd.DataFrame(np.array(st_user_cnt), columns=['Total','Average'])
            df_df_st_user_cnt = df_st_user_cnt.set_index(['Average'])

        # Output

        # Overall India User Analysis Bar chart
        
            df_df_st_user_tab['Brands'] = df_df_st_user_tab['Brands'].astype(str)
            df_df_st_user_tab['User Count'] = df_df_st_user_tab['User Count'].astype(int)

            # bar chart

            #df_df_st_user_tab_fig = px.bar(df_df_st_user_tab , x = 'Brands', y ='User Count', color ='User Count', color_continuous_scale = 'thermal', title = 'User Analysis Chart', height = 500,)
            #df_df_st_user_tab_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
            #st.plotly_chart(df_df_st_user_tab_fig,use_container_width=True)

            #pie chart
            
            df_df_st_user_tab_fig = px.pie(df_df_st_user_tab , names = 'Brands', values ='User Count',hole=0.4) 
            st.plotly_chart(df_df_st_user_tab_fig)

           

        # State wise User Total User calculation Table  
        
           
            st.header(':violet[Total calculation]')

            col3, col4 = st.columns(2)
            with col3:
                st.subheader('User Analysis')
                st.dataframe(df_df_st_user_tab)
            with col4:
                st.subheader('User Count')
                st.dataframe(df_df_st_user_cnt)
        except ValueError:
            st.write(':red[Data not available]')       
             



# Top lists
elif option =='Top ten State - year wise':

    # Select tab
    tab5, tab6 = st.tabs(['Transaction','User'])

    # Overall India Top Transaction 
    with tab5:
        top_tr_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='top_tr_yr')

        # SQL Query
        try:
            # Top Transaction Analysis bar chart query
            cursor.execute(f"SELECT State, SUM(Transaction_amount) As Transaction_amount FROM top_transaction WHERE Year = '{top_tr_yr}' GROUP BY State ORDER BY Transaction_amount DESC LIMIT 10;")
            top_trans_tab = cursor.fetchall()
            df_top_trans_tab = pd.DataFrame(np.array(top_trans_tab), columns=['State', 'Top Transaction amount'])
            df_df_top_trans_tab = df_top_trans_tab.set_index(pd.Index(range(1, len(df_top_trans_tab)+1)))

            # Top Transaction Analysis table query
            cursor.execute(f"SELECT State, SUM(Transaction_amount) as Transaction_amount, SUM(Transaction_count) as Transaction_count FROM top_transaction WHERE Year = '{top_tr_yr}' GROUP BY State ORDER BY Transaction_amount DESC LIMIT 10;")
            top_trans_alys = cursor.fetchall()
            df_top_trans_alys = pd.DataFrame(np.array(top_trans_alys), columns=['State', 'Top Transaction amount','Total Transaction count'])
            df_df_top_trans_alys = df_top_trans_alys.set_index(pd.Index(range(1, len(df_top_trans_alys)+1)))

            # Output  

            # Overall India Transaction Analysis Bar chart 
            df_df_top_trans_tab['State'] = df_df_top_trans_tab['State'].astype(str)
            df_df_top_trans_tab['Top Transaction amount'] = df_df_top_trans_tab['Top Transaction amount'].astype(float)
            df_df_top_trans_tab_fig = px.bar(df_df_top_trans_tab , x = 'State', y ='Top Transaction amount', color ='Top Transaction amount', color_continuous_scale = 'thermal', title = 'Top Transaction Analysis Chart', height = 600,)
            df_df_top_trans_tab_fig.update_layout(title_font=dict(size=35),title_font_color='#6739b7')
            st.plotly_chart(df_df_top_trans_tab_fig,use_container_width=True)

            # Overall India Total Transaction calculation Table 
            st.header(':violet[Top 10 State Transaction Analysis]')
            st.dataframe(df_df_top_trans_alys)

        except ValueError:
            st.write(':red[Data not available]')  


    # Overall India Top User
    with tab6:
        top_us_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='top_us_yr')

        # SQL Query
        
        try:
            # Top User Analysis bar chart query
            cursor.execute(f"SELECT State, SUM(Registered_Users) AS Top_user FROM top_user WHERE Year='{top_us_yr}' GROUP BY State ORDER BY Top_user DESC LIMIT 10;")
            top_user_tab = cursor.fetchall()
            df_top_user_tab = pd.DataFrame(np.array(top_user_tab), columns=['State', 'Total User count'])
            df_df_top_user_tab = df_top_user_tab.set_index(pd.Index(range(1, len(df_top_user_tab)+1)))

            # Output

            # Overall India User Analysis Bar chart
            df_top_user_tab['State'] = df_df_top_user_tab['State'].astype(str)
            df_df_top_user_tab['Total User count'] = df_df_top_user_tab['Total User count'].astype(float)
            df_df_top_user_tab_fig = px.bar(df_df_top_user_tab , x = 'State', y ='Total User count', color ='Total User count', color_continuous_scale = 'thermal', title = 'Top User Analysis Chart', height = 600,)
            df_df_top_user_tab_fig.update_layout(title_font=dict(size=35),title_font_color='#6739b7')
            st.plotly_chart(df_df_top_user_tab_fig,use_container_width=True)

            # Overall India Total Transaction calculation Table 
            st.header(':violet[Top 10 State User Analysis]')
            st.dataframe(df_df_top_user_tab)

        except ValueError:
            st.write(':red[Data not available]')    

else:
    # Select tab
    tab7, tab8 = st.tabs(['Transaction','User'])

    # Overall India Top Transaction 
    with tab7:

        col8, col9 = st.columns(2)
        with col8:
         top1_tr_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='top1_tr_yr')
        with col9:
         top1_tr_qtr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='top1_tr_qtr') 

        # SQL Query
        
        try:
            # Top Transaction Analysis bar chart query
            cursor.execute(f"SELECT State, SUM(Transaction_amount) As Transaction_amount FROM top_transaction WHERE Year = '{top1_tr_yr}' AND Quarter ='{top1_tr_qtr}' GROUP BY State ORDER BY Transaction_amount DESC LIMIT 10;")
            top1_trans_tab = cursor.fetchall()
            df_top1_trans_tab = pd.DataFrame(np.array(top1_trans_tab), columns=['State', 'Top Transaction amount'])
            df_df_top1_trans_tab = df_top1_trans_tab.set_index(pd.Index(range(1, len(df_top1_trans_tab)+1)))

            # Top Transaction Analysis table query
            cursor.execute(f"SELECT State, SUM(Transaction_amount) as Transaction_amount, SUM(Transaction_count) as Transaction_count FROM top_transaction WHERE Year = '{top1_tr_yr}' AND Quarter='{top1_tr_qtr}' GROUP BY State ORDER BY Transaction_amount DESC LIMIT 10;")
            top1_trans_alys = cursor.fetchall()
            df_top1_trans_alys = pd.DataFrame(np.array(top1_trans_alys), columns=['State', 'Top Transaction amount','Total Transaction count'])
            df_df_top1_trans_alys = df_top1_trans_alys.set_index(pd.Index(range(1, len(df_top1_trans_alys)+1)))
            # Output  

            # Overall India Transaction Analysis Bar chart 
            df_df_top1_trans_tab['State'] = df_df_top1_trans_tab['State'].astype(str)
            df_df_top1_trans_tab['Top Transaction amount'] = df_df_top1_trans_tab['Top Transaction amount'].astype(float)
            df_df_top1_trans_tab_fig = px.bar(df_df_top1_trans_tab , x = 'State', y ='Top Transaction amount', color ='Top Transaction amount', color_continuous_scale = 'thermal', title = 'Top 10 States Transaction Analysis Chart', height = 600,)
            df_df_top1_trans_tab_fig.update_layout(title_font=dict(size=35),title_font_color='#6739b7')
            st.plotly_chart(df_df_top1_trans_tab_fig,use_container_width=True)

            # Overall India Total Transaction calculation Table 
            st.header(':violet[Top 10 States]')
            st.subheader('Transaction Analysis')
            st.dataframe(df_df_top1_trans_alys)

             # Top 10 district Transaction Analysis bar chart query
            cursor.execute(f"SELECT District, District_Amount FROM top_Transaction_district WHERE Year = '{top1_tr_yr}' AND Quarter ='{top1_tr_qtr}' ORDER BY District_Amount DESC LIMIT 10;")
            top1_trans_dist_tab = cursor.fetchall()
            df_top1_trans_dist_tab = pd.DataFrame(np.array(top1_trans_dist_tab), columns=['District', 'Transaction amount'])
            df_df_top1_trans_dist_tab = df_top1_trans_dist_tab.set_index(pd.Index(range(1, len(df_top1_trans_dist_tab)+1)))

            # Transaction Analysis table query
            cursor.execute(f"SELECT District, District_Amount, District_trans_count FROM top_Transaction_district WHERE Year = '{top1_tr_yr}' AND Quarter='{top1_tr_qtr}' ORDER BY District_Amount DESC LIMIT 10;")
            top1_trans_dist_alys = cursor.fetchall()
            df_top1_trans_dist_alys = pd.DataFrame(np.array(top1_trans_dist_alys), columns=['District', 'Transaction amount','Transaction count'])
            df_df_top1_trans_dist_alys = df_top1_trans_dist_alys.set_index(pd.Index(range(1, len(df_top1_trans_dist_alys)+1)))
 
            # bar chart
            df_df_top1_trans_dist_tab['District'] = df_df_top1_trans_dist_tab['District'].astype(str)
            df_df_top1_trans_dist_tab['Transaction amount'] = df_df_top1_trans_dist_tab['Transaction amount'].astype(float)
            df_df_top1_trans_dist_tab_fig = px.bar(df_df_top1_trans_dist_tab , x = 'District', y ='Transaction amount', color ='Transaction amount', color_continuous_scale = 'bluered_r', title = 'Top 10 Districts Transaction Analysis Chart', height = 600,)
            df_df_top1_trans_dist_tab_fig.update_layout(title_font=dict(size=35),title_font_color='#6739b7')
            st.plotly_chart(df_df_top1_trans_dist_tab_fig,use_container_width=True)

            # Overall India Total Transaction calculation Table 
            st.header(':violet[Top 10 Districts]')
            st.subheader('Amount Transaction')
            st.dataframe(df_df_top1_trans_dist_alys)

             # Top 10 pincode Transaction Analysis bar chart query
            cursor.execute(f"SELECT Pincode_trans, Transaction_amount FROM top_transaction WHERE Year = '{top1_tr_yr}' AND Quarter ='{top1_tr_qtr}' ORDER BY Transaction_amount DESC LIMIT 10;")
            top1_trans_pin_tab = cursor.fetchall()
            df_top1_trans_pin_tab = pd.DataFrame(np.array(top1_trans_pin_tab), columns=['Pincode', 'Transaction amount'])
            df_df_top1_trans_pin_tab = df_top1_trans_pin_tab.set_index(pd.Index(range(1, len(df_top1_trans_pin_tab)+1)))

            # Transaction Analysis table query
            cursor.execute(f"SELECT Pincode_trans, Transaction_amount, Transaction_count FROM top_transaction WHERE Year = '{top1_tr_yr}' AND Quarter='{top1_tr_qtr}' ORDER BY Transaction_amount DESC LIMIT 10;")
            top1_trans_pin_alys = cursor.fetchall()
            df_top1_trans_pin_alys = pd.DataFrame(np.array(top1_trans_pin_alys), columns=['Pincode', 'Total Transaction amount','Total Transaction count'])
            df_df_top1_trans_pin_alys = df_top1_trans_pin_alys.set_index(pd.Index(range(1, len(df_top1_trans_pin_alys)+1)))
 
            # bar chart
            df_df_top1_trans_pin_tab['Pincode'] = df_df_top1_trans_pin_tab['Pincode'].astype(str)
            df_df_top1_trans_pin_tab['Transaction amount'] = df_df_top1_trans_pin_tab['Transaction amount'].astype(float)
            df_df_top1_trans_pin_tab_fig = px.bar(df_df_top1_trans_pin_tab , x = 'Pincode', y ='Transaction amount', color ='Transaction amount', color_continuous_scale = 'thermal', title = 'Top 10 Pincodes Transaction Analysis Chart', height = 600,)
            df_df_top1_trans_pin_tab_fig.update_layout(title_font=dict(size=35),title_font_color='#6739b7')
            #st.plotly_chart(df_df_top1_trans_pin_tab_fig,use_container_width=True)

            # Overall India Total Transaction calculation Table 
            st.header(':violet[Top 10 Pincodes]')
            st.subheader('Amount Transaction')
            st.dataframe(df_df_top1_trans_pin_alys)
        
        except ValueError:
            st.write(':red[Data not available]')       

         
    # Overall India Top User
    with tab8:

        try:

            col10, col11 = st.columns(2)
            with col10:
             top1_us_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='top1_us_yr')
            with col11:
             top1_us_qtr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='top1_us_qtr') 

            # SQL Query

            # Top User Analysis bar chart query
            cursor.execute(f"SELECT State, SUM(Registered_Users) AS Top_user FROM top_user WHERE Year='{top1_us_yr}' AND Quarter='{top1_us_qtr}' GROUP BY State ORDER BY Top_user DESC LIMIT 10;")
            top1_user_tab = cursor.fetchall()
            df_top1_user_tab = pd.DataFrame(np.array(top1_user_tab), columns=['State', 'Total User count'])
            df_df_top1_user_tab = df_top1_user_tab.set_index(pd.Index(range(1, len(df_top1_user_tab)+1)))

            # Output

            # Overall India User Analysis Bar chart
            df_top1_user_tab['State'] = df_df_top1_user_tab['State'].astype(str)
            df_df_top1_user_tab['Total User count'] = df_df_top1_user_tab['Total User count'].astype(float)
            df_df_top1_user_tab_fig = px.bar(df_df_top1_user_tab , x = 'State', y ='Total User count', color ='Total User count', color_continuous_scale = 'Inferno', title = 'Top 10 States User Analysis Chart', height = 600,)
            df_df_top1_user_tab_fig.update_layout(title_font=dict(size=35),title_font_color='#6739b7')
            st.plotly_chart(df_df_top1_user_tab_fig,use_container_width=True)

             # Overall India Total Transaction calculation Table 
            st.header(':violet[Top 10 States]')
            st.subheader('Users Count')
            st.dataframe(df_df_top1_user_tab)


            # Top 10 district User Analysis bar chart query
            cursor.execute(f"SELECT District, District_Users FROM top_user_district WHERE Year='{top1_us_yr}' AND Quarter='{top1_us_qtr}' ORDER BY District_Users DESC LIMIT 10;")
            top1_user_dist_tab = cursor.fetchall()
            df_top1_user_dist_tab = pd.DataFrame(np.array(top1_user_dist_tab), columns=['District', 'District User count'])
            df_df_top1_user_dist_tab = df_top1_user_dist_tab.set_index(pd.Index(range(1, len(df_top1_user_dist_tab)+1)))

            # Overall India - Pincode User Analysis Bar chart
            df_top1_user_dist_tab['District'] = str(df_top1_user_dist_tab['District'])
            df_top1_user_dist_tab['District'] = df_df_top1_user_dist_tab['District'].astype(str)
            df_df_top1_user_dist_tab['District User count'] = df_df_top1_user_dist_tab['District User count'].astype(float)
            df_df_top1_user_dist_tab_fig = px.bar(df_df_top1_user_dist_tab , x = 'District', y ='District User count', color ='District User count', color_continuous_scale = 'thermal', title = 'Top 10 Districts User Analysis Chart', height = 600,)
            df_df_top1_user_dist_tab_fig.update_layout(title_font=dict(size=35),title_font_color='#6739b7')
            st.plotly_chart(df_df_top1_user_dist_tab_fig,use_container_width=True)

            # Output

            # Overall India Total Transaction calculation Table 
            st.header(':violet[Top 10 Districts]')
            st.subheader('Users Count')
            st.dataframe(df_df_top1_user_dist_tab)

            # Top 10 pincode User Analysis bar chart query
            cursor.execute(f"SELECT Pincode, Registered_Users FROM top_user WHERE Year='{top1_us_yr}' AND Quarter='{top1_us_qtr}' ORDER BY Registered_Users DESC LIMIT 10;")
            top1_user_pin_tab = cursor.fetchall()
            df_top1_user_pin_tab = pd.DataFrame(np.array(top1_user_pin_tab), columns=['Pincode', 'Total User count'])
            df_df_top1_user_pin_tab = df_top1_user_pin_tab.set_index(pd.Index(range(1, len(df_top1_user_pin_tab)+1)))

            # Overall India - Pincode User Analysis Bar chart
            df_top1_user_pin_tab['Pincode'] = str(df_top1_user_pin_tab['Pincode'])
            df_top1_user_pin_tab['Pincode'] = df_df_top1_user_pin_tab['Pincode'].astype(str)
            df_df_top1_user_pin_tab['Total User count'] = df_df_top1_user_pin_tab['Total User count'].astype(float)
            df_df_top1_user_pin_tab_fig = px.bar(df_df_top1_user_pin_tab , x = 'Pincode', y ='Total User count', color ='Total User count', color_continuous_scale = 'twilight', title = 'Top 10 Pincodes User Analysis Chart', height = 600,)
            df_df_top1_user_pin_tab_fig.update_layout(title_font=dict(size=35),title_font_color='#6739b7')
            #st.plotly_chart(df_df_top1_user_pin_tab_fig,use_container_width=True)

            # Output

            # Overall India Total Transaction calculation Table 
            st.header(':violet[Top 10 pincodes]')
            st.subheader('Users Count')
            st.dataframe(df_df_top1_user_pin_tab)

        except ValueError:
            st.write(':red[Data not available]')       
    


