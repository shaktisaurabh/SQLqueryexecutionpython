from flask import Flask,render_template,request,redirect,jsonify
import pandas as pd
import pyodbc
import sqlalchemy as sql
# import os
# import numpy as np
# import csv
import json
# import sqlite3
from flask_cors import CORS
connn=sql.create_engine('mssql+pyodbc://LAPTOP-B7AD4K08\SQLEXPRESS/kerla?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server')
connection = connn.raw_connection()
# con1='mssql+pyodbc://LAPTOP-B7AD4K08\SQLEXPRESS/kerla?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server'
# sql_connect = sqlite3.connect(con1)
app=Flask(__name__)
CORS(app)

@app.route('/cs97',methods=['POST'])
def user():
    if request.method == 'POST':
        column1= request.json['columns'][0]#'columns is actually a key of json whose value is a list containing 2 elements,the first element is stored in
        #variable column1 and the second element is stored in variable column2
        column2=request.json['columns'][1]
        uss=request.json['approx']
        table=request.json['table_name']
        
        # query1=f"select coalesce({column1},'') as 'column24',sum({column2}) as result from {table} group by {column1}"
        query2=f"select * from {table}"
        df1=pd.read_sql(query2,connn)
        s=list(df1[f'{column1}'])#all the contents of the column dynamically passed here column1 will be converted into list and stored in this variable
        print(s)
        k=list(df1[f'{column2}'])
        print(k)
        for val in k:
            if isinstance(val,(int,float)):#if value in column2 is instance of int or float then below things are executed
                
                query1=f"select {column1},{uss}({column2}) as result from {table} group by {column1}"
                print(query1)
                df=pd.read_sql(query1,connn)
                # df[f"{column1}"].fillna('',inplace=True)
                df=df[df[f'{column1}'].notna()]#this is basically a filter wherein only those rows are selected where column1 which has been dynamically selected is not null
                print(df)
                if df.empty:#df.empty gives boolean output,if it is indeed empty the it will give if True:
                   out_data={
                    "response":"no data exists",
                    "status":"failed"
                    }
                else:#else if it is not empty then the following discourse will be followed
                   lem=list(df.columns)#here columnames are picked up and converted into list
                   print(k)
                   col=df.values.tolist()#here values of each row is put together within a list seperated by comma such that a seperate list exists for each row like this [[],[],[]]
                   col.insert(0,lem)#now at 0th position of col list,lem list(containing column names) is placed
                   print(col)
                   out_data={
                    "data":col,
                    "status":"succesfull"
                   }
            else:#else if val above is not instance of int or float then the column on which an aggregation is performed is to be replaced
                query1=f"select {column2},{uss}({column1}) as result from {table} group by {column2}"
                print(query1)
                df=pd.read_sql(query1,connn)
                # df[f"{column2}"].fillna('',inplace=True)
                df=df[df[f'{column2}'].notna()]
                print(df)
                if df.empty:
                   out_data={
                    "response":"no data exists",
                    "status":"failed"
                    }
                else:
                   lem=list(df.columns)
                   print(k)
                   col=df.values.tolist()
                   col.insert(0,lem)
                   print(col)
                   out_data={
                    "data":col,
                    "status":"succesfull"
                   }

            
    return jsonify(out_data)
if __name__=="__main__":
    app.run(debug=True)

    