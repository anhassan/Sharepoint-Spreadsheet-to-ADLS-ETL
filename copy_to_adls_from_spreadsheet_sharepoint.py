# Databricks notebook source
import requests
import json

token_id = "**********************"
site_id = "*********************"
client_id = "*********************"
client_secret = "*********************"

share_point_doc = "Integrations"
share_point_file = "Mapping Table.xlsx"
worksheet = "Sheet1"
table = "Table1"

adls_folder_name = "UKG"

# COMMAND ----------

token_url = "https://login.microsoftonline.com/{}/oauth2/v2.0/token".format(token_id)

token_payload='grant_type=client_credentials&client_id={}&client_secret={}&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default&Content-Type=application%2Fx-www-form-urlencoded'.format(client_id,client_secret)

token_headers = {
  'Content-Type': 'application/x-www-form-urlencoded' 
}

token_response = requests.post(token_url,headers=token_headers,data=token_payload)
access_token = json.loads(token_response.text)["access_token"]


# COMMAND ----------

headers = {"Authorization" : "Bearer {}".format(access_token)}

teams_channel_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/root".format(site_id)
teams_channel_id = json.loads(requests.get(teams_channel_url,headers=headers).text)["id"]

doc_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/items/{}/children".format(site_id,teams_channel_id)
doc_id = [doc["id"] for doc in json.loads(requests.get(doc_url,headers=headers).text)["value"] if doc["name"] == share_point_doc][0]

items_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/items/{}/children".format(site_id,doc_id)
items = json.loads(requests.get(items_url,headers=headers).text)["value"]
item_id = [item["id"] for item in items if item["name"] == share_point_file][0]


# COMMAND ----------

table_rows_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/items/{}/workbook/worksheets/{}/tables/{}/rows".format(site_id,item_id,worksheet,table)
table_metadata_rows = json.loads(requests.get(table_rows_url,headers=headers).text)
table_rows = [row["values"][0] for row in table_metadata_rows["value"] if row["values"][0] != len(row["values"][0])*['']]

table_cols_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/items/{}/workbook/worksheets/{}/tables/{}/columns".format(site_id,item_id,worksheet,table)
table_metadata_cols = json.loads(requests.get(table_cols_url,headers=headers).text)
table_cols = [row["values"][0][0] for row in table_metadata_cols["value"]]

# COMMAND ----------

import pandas as pd

mapping_df = pd.DataFrame(table_rows,columns=table_cols)
file_name = share_point_file[0:share_point_file.rfind(".")]
mapping_df.to_csv("/dbfs/mnt/datalake/{}/{}.csv".format(adls_folder_name,file_name),sep=",",header=True,index=False)
