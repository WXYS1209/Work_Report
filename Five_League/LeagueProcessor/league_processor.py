import numpy as np
import pandas as pd
import re
# from .normal_channel_result_processor import process_normal_result
from .data_cleaner import DataCleaner
from .day_span_processor import DaySpanProcessor
from .file_manager import FileManager

import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
from .other_func import *
from .support_files import *
import emoji
import time
import warnings
import shutil
warnings.filterwarnings('ignore')

class LeagueProcessor:
    def __init__(self, shared_variables):
        self.update_date = shared_variables["Period"]
        self.contains_pre_day = shared_variables["p_d"]
        self.tv_col_start = 10
        self.data_cleaner = DataCleaner()
        self.day_span_processor = DaySpanProcessor(self.data_cleaner)
        self.file_manager = FileManager()
        
    def get_league_info(self, kk):
        league = ["EPL", "Bundesliga", "Ligue 1", "La Liga"][kk-1]
        league_chn = ["英超", "德甲", "法甲", "西甲"][kk-1]
        target_num = [21, 21, 1, 21][kk-1]
        tv_var_num = [7, 7, 5, 7][kk-1]
        tv_cols = np.arange(self.tv_col_start-1, self.tv_col_start + tv_var_num * target_num-1)
        return league, league_chn, target_num, tv_var_num, tv_cols
    
    
    def process_normal_result(self):
        for kk in range(1, 5):
            # Create lists and select the kk-th element (indexing in Python is 0-based, so use kk-1)
            league, league_chn, target_num, tv_var_num, tv_cols = self.get_league_info(kk)
            contains_pre_day = self.contains_pre_day
            data_file = f"D:/csm/InfosysPlusDaily/Export/admin/{league}_game.xlsx"
            df = self.data_cleaner.process_infosys_data("Program", data_file, tv_cols)
            
            df_2025 = df[df['Title'].str.contains("2024/2025|24/25|24-25")]
            
            tmp = self.day_span_processor.get_day_spanning(df_2025, tv_cols, contains_pre_day)
            if len(tmp) == 1:
                print(emoji.emojize(f"  :rocket: {league}: No day span data found."))
                # df_final = tmp[0]
            else:
                print(emoji.emojize(f"  :flying_saucer: {league}: Day span data found. Please get day span data."))
                with open(f'{parent_dir}/infosys_txt/{league}_day_span_ps.txt', 'w', encoding='GBK') as file:
                    file.write(tmp[0])
        print(emoji.emojize(" :stop_sign: Please get all day span data from Infosys and then continue."), end=" ")
        pause_execution()
        
        print(emoji.emojize(f" \U0001F648 Good job! Let's continue:"))
        for kk in range(1, 5):
            league, league_chn, target_num, tv_var_num, tv_cols = self.get_league_info(kk)
            contains_pre_day = self.contains_pre_day
            data_file = f"D:/csm/InfosysPlusDaily/Export/admin/{league}_game.xlsx"
            df = self.data_cleaner.process_infosys_data("Program", data_file, tv_cols)
            
            df_2025 = df[df['Title'].str.contains("2024/2025|24/25|24-25")]
            
            tmp = self.day_span_processor.get_day_spanning(df_2025, tv_cols, contains_pre_day)
            
            df_final = self.day_span_processor.process_day_span_data(tmp, league, tv_cols)
            
            self.file_manager.save_result(df_final, league, "result_other.xlsx")

            print(emoji.emojize(f"  :thumbs_up: {league}: Done! :check_mark:"))
        print(emoji.emojize(":clinking_beer_mugs: Done generating result for all leagues on none-CDP channels! :bottle_with_popping_cork:"))
            # df_final.to_excel("{parent_dir}/Result/result_other.xlsx", index=False)
    
    def process_cdp_result(self):
        update_date = self.update_date
        for kk in range(1, 5):
            # Create lists and select the kk-th element (indexing in Python is 0-based, so use kk-1)
            league, league_chn, target_num, tv_var_num, tv_cols = self.get_league_info(kk)
            contains_pre_day = self.contains_pre_day
            
            cdp_ps = pd.read_excel("D:/wangxiaoyang/Regular_Work/Produce_Report/Football_Program/CDP/cdp_ps_clean.xlsx")
            
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
            
            tmp_cdp = self.day_span_processor.get_day_spanning(
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
            
            self.file_manager.save_result(df_cdp, league, "temp_cdp.xlsx", "TEMP")
            # if not os.path.exists(f'{parent_dir}/TEMP/temp_cdp.xlsx'):
            #     with pd.ExcelWriter(f'{parent_dir}/TEMP/temp_cdp.xlsx', mode='w', engine='openpyxl') as writer:
            #         df_cdp.to_excel(writer, sheet_name=league, index=False)
            # else:
            #     with pd.ExcelWriter(f'{parent_dir}/TEMP/temp_cdp.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            #         df_cdp.to_excel(writer, sheet_name=league, index=False)
            
            # 生成 res 变量，格式化输出
            res = df_cdp.apply(
                lambda row: f"{pd.to_datetime(row['Date']).strftime('%Y%m%d')} " +
                            f"{str(row['code']).zfill(4)} " +
                            f"{str(row['Start']).replace(':', '')} " +
                            f"{str(row['End']).replace(':', '')} " +
                            row['ProgramID'],
                axis=1
            )
            
            file_path = f'{parent_dir}/infosys_txt/{league}_cdp_game_ps.txt'
            
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
            league, league_chn, target_num, tv_var_num, tv_cols = self.get_league_info(kk)
            contains_pre_day = self.contains_pre_day
            
            df_cdp = pd.read_excel(f"{parent_dir}/TEMP/temp_cdp.xlsx", sheet_name=league)
            if len(df_cdp) <= 1:
                df_cdp_game_final = df_cdp
            else:
                # 读取 文件
                data_file = f"D:/csm/InfosysPlusDaily/Export/admin/{league}_cdp_game.xlsx"
                df_cdp_game = self.data_cleaner.process_infosys_data("Vehicles", data_file, tv_cols)
    
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
            
            self.file_manager.save_result(df_cdp_game_final, league, "result_cdp.xlsx")

            # if not os.path.exists(f'{parent_dir}/Result/result_cdp.xlsx'):
            #     with pd.ExcelWriter(f'{parent_dir}/Result/result_cdp.xlsx', mode='w', engine='openpyxl') as writer:
            #         df_cdp_game_final.to_excel(writer, sheet_name=league, index=False)
            # else:
            #     with pd.ExcelWriter(f'{parent_dir}/Result/result_cdp.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            #         df_cdp_game_final.to_excel(writer, sheet_name=league, index=False)
                    # df2.to_excel(writer, sheet_name='Sheet2', index=False)
            print(emoji.emojize(f"  :thumbs_up: {league}: Done! :check_mark:"))
        print(emoji.emojize(":clinking_beer_mugs: Done generating result for all leagues on CDP! :bottle_with_popping_cork:"))
        
    def process_final_result(self):
        wrong_info = False
        update_date = self.update_date
        for kk in range(1, 5):
            # Create lists and select the kk-th element (indexing in Python is 0-based, so use kk-1)
            league, league_chn, target_num, tv_var_num, tv_cols = self.get_league_info(kk)
            contains_pre_day = self.contains_pre_day
            
            df_final = pd.read_excel(f"{parent_dir}/Result/result_other.xlsx", sheet_name=league)
            df_cdp_game_final = pd.read_excel(f"{parent_dir}/Result/result_cdp.xlsx", sheet_name=league)
            df_res = pd.concat([df_final, df_cdp_game_final]).reset_index(drop=True)
            
            df_res['Round'] = df_res['Title'].apply(lambda x: re.search(r"(?<=第).+?(?=轮)", x).group() if re.search(r"(?<=第).+?(?=轮)", x) else "")
            df_res['Round'] = df_res['Round'].apply(replace_chn_num)
            df_res['Round'] = df_res['Round'].apply(lambda x: f"Round {int(x):02d}" if not (pd.isna(x) or x=='') else x)
            
            df_res['Team1'] = df_res['Description'].str.split("VS", expand=True)[0].str.strip()
            df_res['Team2'] = df_res['Description'].str.split("VS", expand=True)[1].str.replace(r"\(直播\)", "", regex=True).str.strip()
            
            # 第一次左连接: Team1 与 team_mapping 匹配
            df_res = df_res.merge(team_mapping, left_on='Team1', right_on='Org', how='left')
            df_res.rename(columns={'Team_Code': 'Home_Team_Code'}, inplace=True)
            
            # 第二次左连接: Team2 与 team_mapping 匹配
            df_res = df_res.merge(team_mapping, left_on='Team2', right_on='Org', how='left')
            df_res.rename(columns={'Team_Code': 'Away_Team_Code'}, inplace=True)
            
            # 第三次左连接: Home_Team_Code 与 code_mapping 匹配
            df_res = df_res.merge(code_mapping, left_on='Home_Team_Code', right_on='Team_Code', how='left')
            df_res.rename(columns={'Eng_Name': 'Home_Name'}, inplace=True)
            
            # 第四次左连接: Away_Team_Code 与 code_mapping 匹配
            df_res = df_res.merge(code_mapping, left_on='Away_Team_Code', right_on='Team_Code', how='left')
            df_res.rename(columns={'Eng_Name': 'Away_Name'}, inplace=True)
            
            # 过滤 sch_seq 中符合 league 条件的行
            sch_seq_league = df_sch[df_sch['League'] == league]
            
            # 左连接 df_res 和 sch_seq_league
            df_res = df_res.merge(
                sch_seq_league,
                left_on=['Home_Name', 'Away_Name', 'Round'],
                right_on=['Home_Team', 'Away_Team', 'Round'],
                how='left'
            )
            
            # 重命名列
            df_res.rename(columns={'Start_x': 'Start', 'Date_x': 'Date'}, inplace=True)
            
            df_res['time_stamp_prog_start'] = df_res.apply(lambda row: convert_tz(row['Date'].strftime("%Y-%m-%d"), row['Start']), axis=1)
            df_res['time_stamp_prog_end'] = df_res.apply(lambda row: convert_tz(row['Date'].strftime("%Y-%m-%d"), row['End']), axis=1)
            
            
            
            # 如果结束时间小于开始时间，给结束时间加一天
            df_res['time_stamp_prog_end'] = np.where(
                df_res['time_stamp_prog_end'] < df_res['time_stamp_prog_start'],
                df_res['time_stamp_prog_end'] + timedelta(days=1),
                df_res['time_stamp_prog_end']
            )
            
            # 计算是否为直播
            df_res['Air'] = np.where(
                (df_res['time_stamp_game_start'] <= df_res['time_stamp_prog_end']) &
                (df_res['time_stamp_game_end'] >= df_res['time_stamp_prog_start']),
                'Live', 'Non-Live'
            )
            
            # 根据是否为直播，设置 Timeslot
            df_res['Timeslot'] = np.where(df_res['Air'] == 'Live', df_res['Live_Timeslot'], '')
            
            # 将 Dur 转换为秒数，假设 get_seconds 函数已定义
            df_res['Dur_s'] = df_res['Dur'].apply(get_seconds)
            
            # 设置 No 列为空
            df_res['No'] = ''
            
            selected_columns = [
                'Round', 'Match_in_Season', 'Dur_s', 'Dur', 'Air', 'Detail', 'No', 'Regions',
                'Title', 'Description', 'Channel', 'Date', 'Weekday', 'Timeslot',
                'Start', 'End'
            ] + list(df_res.columns[tv_cols])
            
            df_res = df_res[selected_columns]
            
            time_columns = ['Start', 'End', 'Dur']
            
            # 将时间列转换为 Excel 时间格式
            for col in time_columns:
                df_res[col] = df_res[col].apply(convert_to_excel_time)
            
            tar_info_league = tar_info['Var_Dig'][np.where(tar_info['Other'] == f'{league}-Match')[0][0]]
            tar_info_league = pd.DataFrame(tar_info_league)
            digits = list(tar_info_league['Digit']) * target_num
            
            df_res = df_res.sort_values(by=['Channel', 'Date', 'Start']).reset_index(drop=True)
            
            # 更新 Regions 列
            df_res['Regions'] = df_res['Regions'].replace({
                'National': 'China National',
                'Guangdong': 'Guangdong Prov.',
                'Tianjin': 'Tianjin Muni.',
                'Beijing': 'Beijing Muni.'
            })
            
            df_res.replace('*', pd.NA, inplace=True)
            
            for i in range(len(digits)):
                # df_res.iloc[:, 16 + i] = df_res.iloc[:, 16 + i].astype('float64').apply(lambda x: round(float(x), digits[i]) if pd.notna(x) else x)
                # df_res.iloc[:, 16 + i] = df_res.iloc[:, 16 + i].apply(lambda x: round(float(x), digits[i]) if pd.notna(x) and isinstance(x, (int, float)) else x)
                df_res.iloc[:, 16 + i] = df_res.iloc[:, 16 + i].apply(lambda x: round(float(x), digits[i]) if pd.notna(x) else x)
            
            # 将 NA 值替换为 "*"
            df_res.fillna('*', inplace=True)
            
            aa = np.where((df_res['Round'] == '*') | (df_res['Match_in_Season'] == '*'))
            df_wrong = df_res.loc[aa[0], ['Title', 'Description', 'Channel']]
            if len(df_wrong) > 0:
                wrong_info = True
                self.file_manager.save_result(df_wrong, league,  "data_with_wrong_info.xlsx", "..")
                # if not os.path.exists(f'{parent_dir}/data_with_wrong_info.xlsx'):
                #     with pd.ExcelWriter(f'{parent_dir}/data_with_wrong_info.xlsx', mode='w', engine='openpyxl') as writer:
                #         df_wrong.to_excel(writer, sheet_name=league, index=False)
                # else:
                #     with pd.ExcelWriter(f'{parent_dir}/data_with_wrong_info.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
                #         df_wrong.to_excel(writer, sheet_name=league, index=False)
                file_path2 = rf'D:\csm\InfosysPlusDaily\Export\admin\{league}_game.xlsx'
                os.startfile(file_path2)
                print(emoji.emojize(f"  :thumbs_down: {league}: There are data with wrong info! Please fix that. :enraged_face:"))
                continue
            
            self.file_manager.save_result(df_res, league, "result.xlsx")
            # if not os.path.exists(f'{parent_dir}/Result/Five_League/result.xlsx'):
            #     with pd.ExcelWriter(f'{parent_dir}/Result/Five_League/result.xlsx', mode='w', engine='openpyxl') as writer:
            #         df_res.to_excel(writer, sheet_name=league, index=False)
            # else:
            #     with pd.ExcelWriter('./Result/Five_League/result.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            #         df_res.to_excel(writer, sheet_name=league, index=False)
            print(emoji.emojize(f"  :thumbs_up: {league}: Done! :check_mark:"))
        
        print(emoji.emojize(":clinking_beer_mugs: Done generating final result for all leagues! :bottle_with_popping_cork:"))
        
        if wrong_info:
            file_path1 = f"{parent_dir}/data_with_wrong_info.xlsx"
            os.startfile(file_path1)
            sys.exit()
        
        file_path = f"{parent_dir}/Result/result.xlsx"
        os.startfile(file_path)
        
        file_name = update_date[1].strftime("%Y%m%d")
        files = os.listdir("D:/wangxiaoyang/Regular_Work/Produce_Report/Reports/Five_League")
        files.sort()
        
        file_date = f"{update_date[1].year}.{update_date[1].month}.{update_date[1].day}"
        
        if file_name not in files:
            shutil.copytree(f"D:/wangxiaoyang/Regular_Work/Produce_Report/Reports/Five_League/{files[len(files)-1]}", f"D:/wangxiaoyang/Regular_Work/Reports/Five_League/{file_name}")
            for ff in [os.path.join(f"D:/wangxiaoyang/Regular_Work/Reports/Five_League/{file_name}", file) for file in os.listdir(f"D:/wangxiaoyang/Regular_Work/Reports/Five_League/{file_name}")]:
                new_file = re.sub(r"\d{4}\.\d{1,2}\.\d{1,2}", file_date, ff)
                os.rename(ff, new_file)
        
        print(emoji.emojize(":trophy: Wonderful! :sports_medal: Marvelous! :1st_place_medal:"))
        print(emoji.emojize("Please finish all reports and then continue with summary."), end=" ")
        pause_exectution()


