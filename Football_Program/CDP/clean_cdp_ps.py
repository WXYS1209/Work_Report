import pandas as pd
from openpyxl import load_workbook
from datetime import datetime, timedelta
import re
import os

# 获取当前脚本文件的目录
script_dir = os.path.dirname(os.path.abspath(__file__))
cdp_ps_path = os.path.join(script_dir, 'cdp_ps.xlsx')
missing_date_path = os.path.join(script_dir, 'missing_date.xlsx')
cdp_missing_ps_path = os.path.join(script_dir, 'cdp_missing_ps.xlsx')

def clean_and_merge_parts(title):
    # 拆分并去除空字符串
    parts = [part for part in title.split("?") if part]
    merged_parts = []

    for part in parts:
        if re.match(r"第\d+轮", part):  # 如果部分是 "第\d+轮"
            if merged_parts:  # 如果前面已经有部分了，合并到前一个部分
                merged_parts[-1] = merged_parts[-1] + part
            else:
                merged_parts.append(part)
        else:
            merged_parts.append(part)

    # 如果有多个部分，保留第一个部分为 Part_1，剩下的合并为 Part_2
    if len(merged_parts) > 1:
        return [merged_parts[0], " ".join(merged_parts[1:])]
    else:
        return [merged_parts[0], None]  # 只有一个部分时，Part_2 为空

# 定义函数，用来提取括号中的内容，并更新 Part_1 和 Part_2
def extract_brackets(row):
    part_1 = row['Part_1']
    part_2 = row['Part_2']

    # 如果 Part_2 是空值，并且 Part_1 中有括号
    if pd.isna(part_2) and re.search(r"\(.*?\)", part_1):
        # 找到括号中的内容
        bracket_content = re.findall(r"\((.*?)\)", part_1)

        if bracket_content:
            # 将括号中的内容放到 Part_2
            row['Part_2'] = bracket_content[0]
            # 去掉括号中的内容，并更新 Part_1
            row['Part_1'] = re.sub(r"\(.*?\)", "", part_1).strip()
            part_2 = row['Part_2']
            part_1 = row['Part_1']
            
    if part_2 and '—' in part_2:
        row['Part_2'] = part_2.replace("—", "VS")
    if part_2 and '-' in part_2:
        row['Part_2'] = part_2.replace("-", "VS")
    return row


def map_uniform(row, comp_df, season_df):
    part_1 = str(row['Part_1'])
    
    comp_match = None
    for org in comp_df['Org']:
        if org in part_1:
            comp_match = comp_df[comp_df['Org'] == org]['Uniform'].values[0]
            break
    
    season_match = None
    for orgg in season_df['Org'].astype(str):
        if orgg in part_1:
            season_match = season_df[season_df['Org'] == orgg]['Uniform'].values[0]
            break
    
    return pd.Series([comp_match, season_match])



team_mapping = pd.read_excel("D:/wangxiaoyang/Regular_Work/support_files/team_mapping_football.xlsx", sheet_name="New")
code_mapping = pd.read_excel("D:/wangxiaoyang/Regular_Work/support_files/team_mapping_football.xlsx",
                             sheet_name="Mapping")

competition_mapping = pd.read_excel("D:/wangxiaoyang/Regular_Work/support_files/competition_mapping.xlsx")
season_mapping = pd.read_excel("D:/wangxiaoyang/Regular_Work/support_files/season_mapping.xlsx")

if __name__ == "__main__":
    schedule1 = pd.read_excel(cdp_ps_path)
    schedule2 = pd.read_excel(cdp_missing_ps_path, sheet_name="Temp")
    if len(schedule2) > 0:
        schedule = pd.concat([schedule1, schedule2])
    else:
        schedule = schedule1
    schedule = schedule.reset_index(drop=True)
    schedule[['Part_1', 'Part_2']] = schedule['Title'].apply(clean_and_merge_parts).apply(pd.Series)
    schedule = schedule.reset_index(drop=True)
    schedule = schedule.apply(extract_brackets, axis=1)
    schedule['Channel'] = 'CCTV CDP Soccer Channel'
    
    schedule[['Competition', 'Season']] = schedule.apply(
        map_uniform, 
        axis=1, 
        comp_df=competition_mapping, 
        season_df=season_mapping
      )
    
    schedule_game = schedule[schedule['Competition'].notna()].copy()
    schedule_game['Date'] = schedule_game['Date'].apply(
      lambda x: x.strftime("%Y-%m-%d")
    )
    schedule_game['Time_Stamp'] = pd.to_datetime(schedule_game['Date'] + ' ' + schedule_game['Time'])
    
    schedule_game['Start'] = schedule_game['Time']
    
    schedule_game['End'] = schedule_game['Time'].apply(lambda x:
        (datetime.strptime(x, '%H:%M:%S') + timedelta(minutes=80)).strftime('%H:%M:%S')
        )
    schedule_game['Dur'] = '1:20:00'
    
    sch_final = schedule_game[[
        'Date', 'Start', 'End', 'Dur', 'Competition', 'Season', 'Part_1', 'Part_2', 'Channel'
    ]]
    try:
        sch_final.to_excel("D:/wangxiaoyang/Regular_Work/Produce_Report/Football_Program/CDP/cdp_ps_clean.xlsx", index=False)
        print("Program sheet in good format.")
    except Exception as e:
        print(f"Error writing the file: {e}")

