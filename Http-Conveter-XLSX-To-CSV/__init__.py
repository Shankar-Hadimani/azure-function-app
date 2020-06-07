import logging

import azure.functions as func
import pandas as pd
import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from azure.identity import ClientSecretCredential



""" Adapted from the second, shorter solution at http://www.codecodex.com/wiki/Calculate_file_name_of_pi#Python
"""
def initialize_storage_account_ad(storage_account_name, client_id, client_secret, tenant_id):
    
    try:  
        global service_client

        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        #client = KeyClient("https://my-vault.vault.azure.net", credential)
        
        # service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        #     "https", storage_account_name), credential=credential)
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", storage_account_name), credential='3whBUWo2AKXUEralvTsBuUELZS/Al89SzdISkjhr4b+T8G5+jS/cS7A46CMIebj9RgWoyVD2F3KPo/Fnf3Nncw==')
        
        return service_client
    
    except Exception as e:
        return e

def read_excel(file_path_name, Sheet_name):
    print('reading')
    excel_df = pd.read_excel(file_path_name, Sheet_name, dtype=str, index_col=None)
    print('read from file')
    return excel_df

def write_csv(df,file_name_path, Sheet_name):
    dest_file_path = file_name_path.replace('.xlsx','').replace('.xls','').replace(' ','')
    print('wriitng')
    df.to_csv( dest_file_path +'_'+ Sheet_name.replace(' ','_') +'.csv', sep=',', encoding='utf-8', index=False)
    print('written')
    return None

def convert_excel_to_csv(file_name):
    file_name="P6-SuperStoreUS-2015"
    file_type="xlsx"
    Sheet_name="Users"
    abs_file_name = "Http-Conveter-XLSX-To-CSV\\P6-SuperStoreUS-2015.xlsx"
    dest_file_path = "Http-Conveter-XLSX-To-CSV\\"
    excel_df = pd.read_excel(abs_file_name, Sheet_name, dtype=str, index_col=None)
    excel_df.to_csv( dest_file_path+ file_name+'_'+Sheet_name +'.csv', sep=',', encoding='utf-8', index=False)

    return None;

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('file_nameOfPi HTTP trigger function processed a request.')

    file_name_param = req.params.get('file_name')

    if file_name_param is not None:
        try:
            file_name = str(file_name_param)
        except ValueError:
            return ValueError('Unnown argument passed the endpoint, please verify')

        # convert xlsx to csv 
        digit_string = convert_excel_to_csv(file_name)

        # connect to adls gen2 and convert xlsx to csv
        conn = initialize_storage_account_ad(storage_account_name='pt008ddakpiadls0001',
        client_id='af2ee70d-e723-4fe6-a9f2-9dc634630c07',
        client_secret='18E12yj-e[=_JZg9U@c@84SGA3qYq7Q',
        tenant_id='63982aff-fb6c-4c22-973b-70e4acfb63e6'
        )
        file_system_client = conn.get_file_system_client(file_system="raw")
        directory_client = file_system_client.get_directory_client("test")
        file_client = directory_client.get_file_client("Market hierarchy.xlsx")

        dest_directory_client = file_system_client.get_directory_client("test/test-output")
        dest_file_client = directory_client.get_file_client("Market hierarchy.xlsx")

        print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
        a_file = [i.name for i in file_system_client.get_paths("") if i.name == 'test/Market hierarchy.xlsx'][0]
        print(a_file)

        #read file
        with open DataLakeFileClient
        read_df = read_excel(a_file,'RX hierarchy')
        # write_data = write_csv(read_df,dest_file_client,'RX hierarchy')


    return func.HttpResponse(
         "Please pass the URL parameter ?file_name= to specify the file_name without .xlsx / .xls extension.",
         status_code=400
    )