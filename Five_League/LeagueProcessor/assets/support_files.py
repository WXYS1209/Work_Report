import pandas as pd
import re
from datetime import timedelta
import json
from assets.other_func import *

# Assuming the files are located in the correct directories, load the necessary datasets
channel_mapping = pd.read_csv("D:/wangxiaoyang/Regular_Work/support_files/channel_mapping.csv")
team_mapping = pd.read_excel("D:/wangxiaoyang/Regular_Work/support_files/team_mapping_football.xlsx", sheet_name="New")
code_mapping = pd.read_excel("D:/wangxiaoyang/Regular_Work/support_files/team_mapping_football.xlsx", sheet_name="Mapping")
df_sch = pd.read_excel("D:/wangxiaoyang/Regular_Work/Produce_Report/Schedule/Five_League/sch_five_league_clean.xlsx")
tar_info = pd.read_csv("D:/wangxiaoyang/Regular_Work/support_files/target_digits.csv")

# Step 2: Apply json.loads (equivalent to fromJSON in R) to the 'Target' and 'Var_Dig' columns
tar_info['Target'] = tar_info['Target'].apply(json.loads)
tar_info['Var_Dig'] = tar_info['Var_Dig'].apply(json.loads)

df_sch['time_stamp_game_start'] = df_sch.apply(
  lambda row: convert_tz(row['Date'], row['Start'], hms=False),
  axis = 1
)
df_sch['time_stamp_game_end'] = df_sch['time_stamp_game_start'].apply(
  lambda x: x + timedelta(hours=2)
)
