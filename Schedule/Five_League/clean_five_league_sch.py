import pandas as pd
from openpyxl import load_workbook
from datetime import datetime, timedelta
import os

# 获取当前脚本文件的目录
script_dir = os.path.dirname(os.path.abspath(__file__))
sch_five_league_path = os.path.join(script_dir, 'sch_five_league.xlsx')
sch_five_league_clean_path = os.path.join(script_dir, 'sch_five_league_clean.xlsx')

def format_time_to_26(time_str):
    if pd.isna(time_str):
        return pd.NA
    tmp = time_str.split(":")
    hh, mm = map(int, tmp[0:2])

    if len(tmp) == 3:
        ss = int(tmp[2])
    else:
        ss = 0

    if hh < 2: 
        hh += 24
        return f'{hh}:{mm:02d}'
    else:
        return time_str.lstrip('0')
      
team_mapping = pd.read_excel("D:/wangxiaoyang/Regular_Work/support_files/team_mapping_football.xlsx", sheet_name="New")
schedule = pd.read_excel(sch_five_league_path)
code_mapping = pd.read_excel("D:/wangxiaoyang/Regular_Work/support_files/team_mapping_football.xlsx", sheet_name="Mapping")

schedule = schedule[schedule['Time'].notna()]
schedule_with_codes = schedule.merge(
    team_mapping[['Org', 'Team_Code']], how='left', left_on='Home Team', right_on='Org'
).rename(columns={'Team_Code': 'Home_Team_Code'}).merge(
    team_mapping[['Org', 'Team_Code']], how='left', left_on='Away Team', right_on='Org'
).rename(columns={'Team_Code': 'Away_Team_Code'})

if schedule_with_codes['Home_Team_Code'].isna().any() or schedule_with_codes['Away_Team_Code'].isna().any():
    print("New team name.")
    
sch = schedule_with_codes.merge(
    code_mapping, how='left', left_on='Home_Team_Code', right_on='Team_Code'
).rename(columns={'Eng_Name': 'Home_Name'}).merge(
    code_mapping, how='left', left_on='Away_Team_Code', right_on='Team_Code'
).rename(columns={'Eng_Name': 'Away_Name'})

# 创建 "vs." 字符串并处理其他列
sch['Detail'] = sch['Home_Name'] + " vs. " + sch['Away_Name']
sch['Date_Org'] = sch['Date'].apply(lambda x: 
    x.strftime('%Y-%m-%d')
)
sch['Time_Stamp'] = pd.to_datetime(sch.Date_Org +' ' + sch.Time)

# 处理时间格式
sch['Start'] = sch['Time'].apply(lambda x: 
    format_time_to_26(x))

sch['Date'] = sch.apply(lambda row: 
    (row['Date'] - timedelta(days=1)).strftime('%Y-%m-%d')
    if int(row['Start'].split(":")[0]) >= 24 
    else row['Date'].strftime('%Y-%m-%d'), axis=1
)

# 计算结束时间
sch['End'] = sch['Time'].apply(lambda x:
    format_time_to_26( (datetime.strptime(x, '%H:%M') + timedelta(hours=2)).strftime('%H:%M') )
    )

# 计算 Live Timeslot
sch['Live_Timeslot'] = sch['Start'] + '-' + sch['End']

sch['Home_Team'] = sch['Home_Name']
sch['Away_Team'] = sch['Away_Name']


# 按赛事和开始时间排序，计算轮次
sch_final = sch.groupby('League', group_keys=False).apply(
    lambda x: x.sort_values(by=['Time_Stamp']).reset_index(drop=True)
).reset_index(drop=True)


sch_final['Match_in_Season'] = sch_final.groupby('League').cumcount() + 1

# 选择需要的列
sch_final = sch_final[[
    'League', 'Start', 'Home_Team', 'Away_Team', 'Date', 'Detail', 'Live_Timeslot', 'Round', 'Match_in_Season'
]]

sch_final.to_excel(sch_five_league_clean_path, index=False)
print("Schedule in good format.")

