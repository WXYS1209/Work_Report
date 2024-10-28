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

class SummaryProcessor:
    def __init__(self, shared_variables):
        self.update_date = shared_variables["Period"]
        self.contains_pre_day = shared_variables["p_d"]
        self.tv_col_start = 10
        self.data_cleaner = DataCleaner()
        self.day_span_processor = DaySpanProcessor(self.data_cleaner)
        self.file_manager = FileManager()
    
    def get_updated_df(df, update_date, league=""):
        # 找到 title_row 的位置
        title_row = df.apply(lambda x: any(x.str.contains("date", case=False, na=False)) and any(x.str.contains("start", case=False, na=False)), axis=1).idxmax()
        
        # 定义 target_row
        target_row = title_row - 1
        
        # 转置 target_row 行并填充数据
        tp_target_df = pd.DataFrame(df.iloc[target_row].values).T
        
        tp_target_df_filled = tp_target_df.ffill(axis=1)
        
        df.iloc[target_row] = tp_target_df_filled.values.flatten()
        
        # 定义 tv_start_col
        tv_start_col = np.where(df.iloc[target_row].str.contains("Targets", na=False))[0][0] + 1
        
        # 构建 ColNames
        ColNames = list(df.iloc[title_row, :tv_start_col]) + \
                   [f"{df.iloc[target_row, i]} {df.iloc[title_row, i]}" for i in range(tv_start_col, df.shape[1])]
        
        # 设置列名
        df.columns = ColNames
        
        # 删除 title_row 及之前的行
        df = df.iloc[title_row+1:].copy()
        
        df = df[
            (df['Date'] >= pd.to_datetime(update_date[0])) & 
            (df['Date'] <= pd.to_datetime(update_date[1]))
        ]
        
        # 选择前 21 列并添加 League 列
        df = df.iloc[:, :21].copy().reset_index(drop=True)
        df['League'] = league
            
        return df
