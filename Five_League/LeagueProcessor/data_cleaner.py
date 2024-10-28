import pandas as pd
import re

class DataCleaner:
    def process_infosys_data(self, data_type, data_file, tv_cols):
        if data_type == "Program":
            """读取并清洗数据"""
            df_org = pd.read_excel(data_file, header=None, sheet_name='节目')
            df_org.iloc[1, :] = df_org.iloc[1, :].ffill()
            df_org.iloc[2, tv_cols] = df_org.iloc[1, tv_cols] + " " + df_org.iloc[2, tv_cols]
            df_org.columns = df_org.iloc[2]
            df_org = df_org.drop(index=[0, 1, 2])
            df_org['地区'] = df_org['地区'].ffill()
            
            df = df_org[
                (df_org['地区'] != 'DUMMY') & (df_org['Title'] != '摘要')
            ].copy().reset_index(drop=True)
            
            df['Regions'] = df['地区'].str.split(r"\(", n=1, expand=True)[0]
            df['Regions'] = df['地区'].str[0] + df['Regions'].str[1:].str.lower()
            
            df = df[
                df.apply(
                    lambda row: re.search(row['Regions'], row['Channel']) is not None or 
                    (row['Regions'] == "National" and "CCTV" in row['Channel']) or
                    (row['Regions'] == "Fujian" and "FJ" in row['Channel']), axis=1
                )
            ]
            
        elif data_type == "Crosstab":
            df_org = pd.read_excel(data_file, header=None, sheet_name='交互分析')
            df_org.iloc[2, :] = df_org.iloc[2, :].ffill()
            df_org.iloc[3, tv_cols - 6] = df_org.iloc[2, tv_cols - 6] + " " + df_org.iloc[3, tv_cols - 6]
            df_org.columns = df_org.iloc[3]
            df_org = df_org.drop(index=[0, 1, 2, 3])
            df_org['地区'] = df_org['地区'].ffill()
            
            # 清洗和过滤数据
            df = df_org[
                (df_org['地区'] != 'DUMMY') & (df_org['Title'] != '摘要') & (df_org['Title'].notna())
            ].copy().reset_index(drop=True)
            
            df['Regions'] = df['地区'].str.split(r"\(", n=1, expand=True)[0]
            df['Regions'] = df['地区'].str[0] + df['Regions'].str[1:].str.lower()
            
            df = df[
                df.apply(lambda row: re.search(row['Regions'], row['Title']) is not None or
                            (row['Regions'] == "National" and "CCTV" in row['Title']) or
                            (row['Regions'] == "Fujian" and "FJ" in row['Channel']), axis=1)
            ]
            df = df.drop(columns=['地区', '摘要', 'Regions'])
            
        elif data_type == "Vehicles":
            df_org = pd.read_excel(data_file, header=None, sheet_name='载体')
            df_org.iloc[1, :] = df_org.iloc[1, :].ffill()
            
            df_org.iloc[2, 7:] = df_org.iloc[1, 7:] + " " + df_org.iloc[2, 7:] # 7: 载体的tv_cols
            df_org.columns = df_org.iloc[2]
            df_org = df_org.drop(index=[0, 1, 2])
            df_org['地区'] = df_org['地区'].ffill()
            
            df_org.rename(columns={df_org.columns[1]: 'ProgramID'}, inplace=True) # 设置 ProgramID 列名
            df_org['Date'] = pd.to_datetime(df_org['Date'])
            
            # 过滤并处理数据
            df = df_org[
                (df_org['地区'] != 'DUMMY') & 
                (df_org['ProgramID'] != '摘要')
            ].copy().reset_index(drop=True)
            
            # 处理 Regions 列
            df['Regions'] = df['地区'].str.split(r'\(', expand=True)[0]
            df['Regions'] = df['地区'].str[0] + df['Regions'].str[1:].str.lower()
            
            # 使用 mapply 替换为 numpy 的向量化操作
            df = df[
                df.apply(lambda row: re.search(row['Regions'], row['ProgramID']) is not None or
                                  (row['Regions'] == "National" and re.search("CCTV|Qinghai", row['ProgramID']) is not None) or
                                  (row['Regions'] == "Fujian" and re.search("FJ", row['ProgramID']) is not None), axis=1)
            ]
            
            df = df.drop(columns=['地区'])
        else:
            return None
          
        return df
