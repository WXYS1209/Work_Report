import pandas as pd
import re
import numpy as np
from datetime import datetime, timedelta

class DaySpanProcessor:
    def __init__(self, data_cleaner):
        self.data_cleaner = data_cleaner
    
    def get_day_spanning( self,
        df, tv_cols, contain_pre_day=True,
        title='Title', desc='Description',
        start='Program Start time', dur='Duration', end='Program End Time',
        date='Date', weekday='Weekday', channel='Channel'):
          
        # 读取映射表
        mapping = pd.read_csv("D:/wangxiaoyang/Regular_Work/support_files/channel_mapping.csv")
        
        # 第一步：创建新列，基于传入的列名
        df['Title'] = df[title]
        df['Description'] = df[desc]
        df['Channel'] = df[channel]
        df['Date'] = pd.to_datetime(df[date])
        df['Weekday'] = df[weekday]
        df['Start'] = df[start]
        df['Dur'] = df[dur]
        df['End'] = df[end]
        
        cols = [df.columns.get_loc(c) for c in ['Regions', 'Title', 'Description', 'Channel', 'Date', 'Weekday', 'Start', 'End', 'Dur']]
        cols.extend(tv_cols)
        df = df.iloc[:, cols]
        
        # 第二步：过滤掉特定的行，处理 "26:00:00" 和 "2:00:00"
        df_normal = df[(df['End'] != "26:00:00") & (df['Start'] != "02:00:00")].copy()
        
        # 如果 `contain_pre_day` 为 True，则过滤掉最小日期的数据
        if contain_pre_day:
            df_normal = df_normal[df_normal['Date'] != df_normal['Date'].min()]
        
        # 第三步：处理特殊情况，筛选时间为 "26:00:00" 或 "2:00:00" 的行
        df_special = df[(df['End'] == "26:00:00") | (df['Start'] == "02:00:00")].copy()
        
        # 如果只有1行或0行，直接返回结果
        if len(df_special) <= 1:
            return [df_normal]
        
        # 第四步：对特殊情况进行排序和进一步处理
        df_special = df_special.sort_values(by=['Channel', 'Date', 'Start'])
        
        df_special['indicator'] = np.where(df_special['Start'] == "2:00:00", 2, 
                                           np.where(df_special['End'] == "26:00:00", 1, 0))
        
        df_special['Date_lead'] = df_special['Date'].shift(-1)
        df_special['Date_next'] = df_special['Date'] + timedelta(days=1)
        df_special['Title_lead'] = df_special['Title'].shift(-1)
        
        df_special['day_span'] = (df_special['Title'] == df_special['Title_lead']) & (df_special['Date_lead'] == df_special['Date_next']) & (df_special['End'] == "26:00:00")
        df_special = df_special.reset_index(drop=True)
        
        # 第五步：检查哪些行需要处理
        rows_to_check = []
        name_list = []
        
        for i in range(len(df_special)):
            if df_special.iloc[i]['day_span']:
                rows_to_check.extend([i, i+1])
                name_list.extend([f"{df_special.iloc[i]['Date'].strftime('%Y-%m-%d')} {df_special.iloc[i]['Channel']}"] * 2)
        rows_to_check = list(set(rows_to_check))
        
        # 第六步：与映射数据合并并格式化结果
        res_df = df_special.iloc[rows_to_check].merge(mapping, how='left', left_on='Channel', right_on='channel')
        res_df['name_list'] = name_list
        # 调整 End 时间并格式化结果文本
        res_df['End'] = np.where(res_df['End'] == "26:00:00", "25:59:59", res_df['End'])
        
        res = res_df.apply(
            lambda row: f"{row['Date'].strftime('%Y%m%d')} {row['code']:04} "
                        f"{row['Start'].replace(':', '')} {row['End'].replace(':', '')} {row['name_list']}",
            axis=1
        ).tolist()
        
        # 第七步：调整持续时间并合并 DataFrames
        selected_columns = ['Regions', 'Title', 'Description', 'Channel', 'Date', 'Weekday', 'Start', 'End', 'Dur']
        df_ds = res_df[selected_columns + list(res_df.columns[tv_cols])].copy().reset_index(drop=True)
    
        for i in range(0, len(df_ds), 2):
            # 调整 End 时间
            df_ds.at[i, 'End'] = df_ds.at[i+1, 'End']
            
            # 调整持续时间
            dur_1 = pd.to_timedelta(df_ds.iloc[i]['Dur'])
            dur_2 = pd.to_timedelta(df_ds.iloc[i+1]['Dur'])
            total_duration = dur_1 + dur_2
            time_obj = (datetime.min + total_duration).time()
            df_ds.at[i, 'Dur'] = time_obj.strftime("%H:%M:%S")
    
        # 删除跨天的数据
        df_normal = pd.concat([df_normal, df_special.drop(index=rows_to_check)])
        df_normal = df_normal[selected_columns + list(res_df.columns[tv_cols])].copy().reset_index(drop=True)
        
        # 返回最终结果
        return ['\n'.join(res),
            df_normal,
            df_ds.iloc[::2]  # 每隔一行取一次
               ]

    def process_day_span_data(self, tmp, league, tv_cols):
        """处理跨天数据并返回最终的 DataFrame"""
        if len(tmp) == 1:
            df_final = tmp[0]
        else:
            ds_file = f"D:/csm/InfosysPlusDaily/Export/admin/{league}_day_span.xlsx"
            df_ds = self.data_cleaner.process_infosys_data("Crosstab", ds_file, tv_cols)
            
            # 处理 tmp[[3]] 数据帧
            data_ds = tmp[2].copy()
            data_ds['indicator'] = data_ds['Date'].astype(str) + " " + data_ds['Channel']
            
            # 合并 df_ds 和 data_ds，依据 'indicator == Title'
            data_ds = pd.merge(data_ds, df_ds, how='left', left_on='indicator', right_on='Title')
            
            # 删除 tv_cols 对应的列和 'indicator' 列
            data_ds = data_ds.drop(columns=[data_ds.columns[i] for i in tv_cols] + ['indicator', 'Title_y'])
            
            # 将列名设置为 tmp[[2]] 的列名
            data_ds.columns = tmp[1].columns
            data_ds.iloc[:, tv_cols] = data_ds.iloc[:, tv_cols].replace(".", 0)
            data_ds.iloc[:, tv_cols] = data_ds.iloc[:, tv_cols].astype(float)
            
            # 合并 tmp[[2]] 和 data_ds
            df_final = pd.concat([tmp[1], data_ds], ignore_index=True)
        
        return df_final
