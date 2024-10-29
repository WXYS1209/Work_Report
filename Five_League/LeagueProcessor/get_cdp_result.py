import pandas as pd
import re
from assets.get_day_span_func import *
import argparse
import numpy as np
from assets.other_func import *
from assets.support_files import *
import os
from openpyxl import Workbook
import emoji

parser = argparse.ArgumentParser(description='Get CDP program sheet.')
# parser.add_argument('-l', '--league', type=int, required=True, help='EPL:1, Bundesliga:2, Ligue 1:3, La Liga:4')
parser.add_argument('-d', '--date', nargs=2, type=str, required=True, help='start_date end_date')
parser.add_argument('--pd', action='store_true', help='Whether contain previous day.' )
args = parser.parse_args()

print(emoji.emojize(":brain: Now let's generate result for CDP: "))
print(emoji.emojize("  :exploding_head: Getting CDP program sheet for InfoSys......"))

# Set contains_pre_day flag
contains_pre_day = args.pd
# kk = args.league
d1 = args.date[0]
d2 = args.date[1]

# Define update_date as a list of two dates
update_date = pd.to_datetime([d1, d2])

for kk in range(1, 5):
    # Create lists and select the kk-th element (indexing in Python is 0-based, so use kk-1)
    league = ["EPL", "Bundesliga", "Ligue 1", "La Liga"][kk-1]
    league_chn = ["英超", "德甲", "法甲", "西甲"][kk-1]
    
    # Define tv_col_start
    tv_col_start = 10
    
    # Select corresponding target_num and tv_var_num
    target_num = [21, 21, 1, 21][kk-1]
    tv_var_num = [7, 7, 5, 7][kk-1]
    
    # Generate tv_cols using numpy
    tv_cols = np.arange(tv_col_start-1, tv_col_start + tv_var_num * target_num-1)
    
    cdp_ps = pd.read_excel("D:/wangxiaoyang/Regular_Work/Produce_Report/CDP/cdp_ps_clean.xlsx")
    
    # 筛选 cdp_league 数据
    cdp_league = cdp_ps[
        (cdp_ps['Competition'] == league_chn) &
        (cdp_ps['Season'] == '2024-25赛季') &
        (pd.to_datetime(cdp_ps['Date']) >= update_date[0]) &
        (pd.to_datetime(cdp_ps['Date']) <= update_date[1])
    ].copy()
    
    cdp_league['Program'] = cdp_league['Part_1']
    
    cdp_league['Description'] = cdp_league['Part_2']
    
    cdp_league['Weekday'] = ""
    cdp_league['Regions'] = "National"
    cdp_league = cdp_league[['Regions', 'Date', 'Weekday', 'Start', 'End', 'Dur', 'Channel', 'Program', 'Description']]
    cdp_league = cdp_league.drop_duplicates()
    
    cdp_league['End'] = np.where(
        cdp_league['Description'].str.contains("直播"),
        cdp_league['End'].apply(lambda x: convert_time(get_seconds(x, durr=False) + 50 * 60)),
        cdp_league['End']
    )
    
    cdp_league['Dur'] = np.where(
        cdp_league['Description'].str.contains("直播"),
        "2:10:00",
        cdp_league['Dur']
    )
    
    cdp_league['Program'] = np.where(
        cdp_league['Description'].str.contains("直播"),
        cdp_league['Program'] + "(直播)",
        cdp_league['Program']
    )
    
    cdp_league['Description'] = np.where(
        cdp_league['Description'].str.contains("直播"),
        cdp_league['Description'].replace("(直播)", ""),
        cdp_league['Description']
    )
    
    tmp_cdp = get_day_spanning(
        cdp_league,
        '',
        contains_pre_day,
        'Program',
        'Description',
        'Start',
        'Dur',
        'End',
        'Date',
        'Weekday',
        'Channel')
    
    if len(tmp_cdp) == 1:
        df_cdp = tmp_cdp[0]
    
    # 进行 left join，使用 merge 进行合并
    df_cdp = df_cdp.merge(channel_mapping, how='left', left_on='Channel', right_on='channel')
    
    # 创建 ProgramID 列
    df_cdp['ProgramID'] = df_cdp['Channel'] + ' ' + df_cdp['Date'].astype(str) + ' ' + df_cdp['Start']
    
    if not os.path.exists('./TEMP/temp_cdp.xlsx'):
        with pd.ExcelWriter('./TEMP/temp_cdp.xlsx', mode='w', engine='openpyxl') as writer:
            df_cdp.to_excel(writer, sheet_name=league, index=False)
    else:
        with pd.ExcelWriter('./TEMP/temp_cdp.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df_cdp.to_excel(writer, sheet_name=league, index=False)
    
    # 生成 res 变量，格式化输出
    res = df_cdp.apply(
        lambda row: f"{pd.to_datetime(row['Date']).strftime('%Y%m%d')} " +
                    f"{str(row['code']).zfill(4)} " +
                    f"{str(row['Start']).replace(':', '')} " +
                    f"{str(row['End']).replace(':', '')} " +
                    row['ProgramID'],
        axis=1
    )
    
    file_path = f'../infosys_txt/{league}_cdp_game_ps.txt'
    
    res.to_csv(file_path, 
               sep='\t',
               index=False,
               header=False,
               quoting=False,
               encoding="GBK")
print(emoji.emojize("  \U0001F607 Done!"))

print(emoji.emojize(" :stop_sign: Now please get data from Infosys for CDP."), end=" ")

pause_execution()
print(emoji.emojize(f" \U0001F648 Good job! Let's continue:"))

for kk in range(1, 5):
    league = ["EPL", "Bundesliga", "Ligue 1", "La Liga"][kk-1]
    league_chn = ["英超", "德甲", "法甲", "西甲"][kk-1]
    
    # Define tv_col_start
    tv_col_start = 10
    
    # Select corresponding target_num and tv_var_num
    target_num = [21, 21, 1, 21][kk-1]
    tv_var_num = [7, 7, 5, 7][kk-1]
    
    # Generate tv_cols using numpy
    tv_cols = np.arange(tv_col_start-1, tv_col_start + tv_var_num * target_num-1)
    
    df_cdp = pd.read_excel(f"./TEMP/temp_cdp.xlsx", sheet_name=league)
    if len(df_cdp) <= 1:
        df_cdp_game_final = df_cdp
    else:
        # 读取 文件
        data_file = f"D:/csm/InfosysPlusDaily/Export/admin/{league}_cdp_game.xlsx"
        df_cdp_org = pd.read_excel(data_file, header=None, sheet_name='载体')
        df_cdp_org.iloc[1,] = df_cdp_org.iloc[1,].ffill()
        
        df_cdp_org.iloc[2, 7:] = df_cdp_org.iloc[1, 7:] + " " + df_cdp_org.iloc[2, 7:] # 7: 载体的tv_cols
        df_cdp_org.columns = df_cdp_org.iloc[2]
        df_cdp_org = df_cdp_org.drop(index=[0, 1, 2])
        df_cdp_org['地区'] = df_cdp_org['地区'].ffill()
        
        df_cdp_org.rename(columns={df_cdp_org.columns[1]: 'ProgramID'}, inplace=True) # 设置 ProgramID 列名
        df_cdp_org['Date'] = pd.to_datetime(df_cdp_org['Date'])
        
        # 过滤并处理数据
        df_cdp_game = df_cdp_org[
            (df_cdp_org['地区'] != 'DUMMY') & 
            (df_cdp_org['ProgramID'] != '摘要')
        ].copy().reset_index(drop=True)
        
        # 处理 Regions 列
        df_cdp_game['Regions'] = df_cdp_game['地区'].str.split(r'\(', expand=True)[0]
        df_cdp_game['Regions'] = df_cdp_game['地区'].str[0] + df_cdp_game['Regions'].str[1:].str.lower()
        
        # 使用 mapply 替换为 numpy 的向量化操作
        df_cdp_game = df_cdp_game[
            df_cdp_game.apply(lambda row: re.search(row['Regions'], row['ProgramID']) is not None or
                              (row['Regions'] == "National" and re.search("CCTV|Qinghai", row['ProgramID']) is not None) or
                              (row['Regions'] == "Fujian" and re.search("FJ", row['ProgramID']) is not None), axis=1)
        ]
        
        # 删除 '地区' 列
        df_cdp_game = df_cdp_game.drop(columns=['地区'])
        
        # 使用 left join 合并 df_cdp
        df_cdp_game_final = pd.merge(
            df_cdp_game,
            df_cdp[['Title', 'Description', 'Dur', 'ProgramID']],
            on='ProgramID',
            how='left'
        )
        
        # 处理 Start 和 End 列
        df_cdp_game_final['Start'] = df_cdp_game_final['Start Time']
        df_cdp_game_final['End'] = df_cdp_game_final['End Time']
        
        # 选择指定的列
        columns_to_select = ['Regions', 'Title', 'Description', 'Channel', 'Date', 'Weekday', 'Start', 'End', 'Dur']
        columns_to_select.extend(list(df_cdp_game_final.columns[6:(6 + target_num * tv_var_num)]))
        df_cdp_game_final = df_cdp_game_final[columns_to_select]
        
        # 将某些列中的 '.' 替换为 0
        df_cdp_game_final.iloc[:, tv_cols] = df_cdp_game_final.iloc[:, tv_cols].replace('.', 0)
        df_cdp_game_final.iloc[:,tv_cols] = df_cdp_game_final.iloc[:,tv_cols].astype(float)
        
    # if not os.path.exists('./Result'):
    #     os.mkdir('./Result')
    
    if not os.path.exists('./Result/Five_League/result_cdp.xlsx'):
        with pd.ExcelWriter('./Result/Five_League/result_cdp.xlsx', mode='w', engine='openpyxl') as writer:
            df_cdp_game_final.to_excel(writer, sheet_name=league, index=False)
    else:
        with pd.ExcelWriter('./Result/Five_League/result_cdp.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df_cdp_game_final.to_excel(writer, sheet_name=league, index=False)
            # df2.to_excel(writer, sheet_name='Sheet2', index=False)
    print(emoji.emojize(f"  :thumbs_up: {league}: Done! :check_mark:"))
print(emoji.emojize(":clinking_beer_mugs: Done generating result for all leagues on CDP! :bottle_with_popping_cork:"))
