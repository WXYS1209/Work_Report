import pandas as pd
import sys
import os
sys.path.append('D:/wangxiaoyang/Regular_Work/Produce_Report/Auto_Flow/assets')  # Replace with the actual path
from other_func import *  # Replace with the actual file and function/class names

# 获取当前脚本文件的目录
script_dir = os.path.dirname(os.path.abspath(__file__))
cdp_ps_path = os.path.join(script_dir, 'cdp_ps.xlsx')
missing_date_path = os.path.join(script_dir, 'missing_date.xlsx')
cdp_missing_ps_path = os.path.join(script_dir, 'cdp_missing_ps.xlsx')

df1 = pd.DataFrame(columns=["Date", "Time", "Title"])
# df1

import argparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium.common.exceptions import StaleElementReferenceException
import pandas as pd
from datetime import datetime, timedelta

def positive_int(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError(f"Invalid value: {value}. Must be a positive integer.")
    return ivalue
  
parser = argparse.ArgumentParser(description='Crawl CDP program.')
parser.add_argument('-n', '--num_week', type=positive_int, default=1)

args = parser.parse_args()
week_num = args.num_week

def get_df(df_res, driver):
    # 重新查找所有包含节目单的 <p> 元素
    program_elements = driver.find_elements(By.CSS_SELECTOR, 'div#liveepg p.normal')
    program_time = []
    program_name = []
    program_date = []
    # 遍历并提取每个节目的日期、时间和名称
    for program in program_elements:
        # 尝试重新获取节目时间和名称
        try:
            # 获取节目时间
            time_element = program.find_element(By.CSS_SELECTOR, 'span.epgtime')
            program_time.append(time_element.text)
            
            # 获取节目名称
            name_element = program.find_elements(By.TAG_NAME, 'span')[1]
            program_name.append(name_element.text)
    
            # 获取节目日期 (从title属性中提取日期信息)
            program_date.append(program.get_attribute('title').split(' ')[0])
    
            # 打印日期、时间和节目名称
            # print(f"日期: {program_date}, 时间: {program_time}, 节目: {program_name}")
        except StaleElementReferenceException:
            print("元素已过期，需要重新加载页面元素")
            
    data = {
        'program_date': program_date,
        'program_time': program_time,
        'program_name': program_name
    }
    
    # 创建 DataFrame
    df = pd.DataFrame(data)

    # 将 program_date 转换为 datetime 格式
    df['program_date'] = pd.to_datetime(df['program_date'], format='%m月%d日')

    # 创建新的 df_new DataFrame
    df_new = pd.DataFrame()

    # 填充 df_new
    df_new['Date'] = df['program_date'].apply(lambda x: x.replace(year=2024))
    df_new['Time'] = pd.to_datetime(df_new['Date'].dt.strftime('%Y-%m-%d') + ' ' + df['program_time'])
    # df_new['end'] = df_new['start'] + timedelta(minutes=80)
    df_new['Date'] = df_new['Date'].dt.strftime('%Y-%m-%d')
    df_new['Time'] = df_new['Time'].dt.strftime('%H:%M:%S')
    # df_new['end'] = df_new['end'].dt.strftime('%H:%M:%S')
    df_new['Title'] = df['program_name']
    return pd.concat([df_res, df_new])

def get_data_for_week(driver, df):
    for i in range(7):
        weekday_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, f"weekday_{i}"))
        )
        weekday_button.click()
        time.sleep(1)
        df = get_df(df, driver)
    return df
    
# 设置ChromeDriver路径
driver_path = r'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe'  # 替换为你下载的ChromeDriver路径
service = Service(driver_path)

# 启动Chrome浏览器
driver = webdriver.Chrome(service=service)

# 打开目标网址
url = 'http://epg.tv.cn/soccer'  # 替换为实际节目单的URL
driver.get(url)

# 等待页面完全加载
time.sleep(1)
# df2 = get_df(df1, driver)
df2 = df1.copy()

for i in range(week_num):
    df2 = get_data_for_week(driver, df2)
    ## Last week
    last_week_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "upweek"))
    )
    last_week_button.click()
    time.sleep(1)
    # df2 = get_data_for_week(driver, df2)

'''next_week_button = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.ID, "upweek"))
)
next_week_button.click()
time.sleep(1)

for i in range(7):
    next_week_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, f"weekday_{i}"))
    )
    next_week_button.click()
    time.sleep(1)
    df2 = get_df(df2, driver)'''
    
# 关闭浏览器
driver.quit()

df2.index = range(len(df2))

df2['DateTime'] = pd.to_datetime(df2['Date'] + ' ' + df2['Time'])
df2['Date'] = pd.to_datetime(df2['Date'])

# Deal 当前没有节目安排！

ddl = datetime.today().strftime("%Y-%m-%d") + ' ' + '02:00:00'
df_final = df2.copy()
df_final = df_final.drop_duplicates().sort_values(by=['DateTime'])
df_final = df_final[ (df_final['DateTime'] < ddl) ].copy()
df_final = df_final[['Date', 'Time', 'Title']]

df_final.to_excel(cdp_ps_path, index=False)

dff = df_final[df_final['Title'].apply(lambda x: '当前没有节目安排' in x)]
missing_date = pd.unique(dff['Date'])

if len(missing_date) > 0:
    pd.DataFrame(missing_date).to_excel(missing_date_path, index=False)
    pause_execution()
    df_missing = pd.read_excel(cdp_missing_ps_path, sheet_name="All")
    # df_missing['Date'] = pd.to_datetime(df_missing['Date'])
    # Find dates in `missing_date` that are not in `df_missing['Date']`
    missing_in_df = [date for date in missing_date if date not in df_missing['Date'].values]
    if len(missing_in_df) > 0:
        print(f"Still missing programs in: {missing_in_df}.")
        pause_execution()
        
    df_missing = df_missing[df_missing['Date'].isin(missing_date)]
    with pd.ExcelWriter(cdp_missing_ps_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df_missing.to_excel(writer, sheet_name="Temp", index=False)
else:
    with pd.ExcelWriter(cdp_missing_ps_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df1.to_excel(writer, sheet_name="Temp", index=False)
    
    
