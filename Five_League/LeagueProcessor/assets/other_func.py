import urllib
import urllib.parse
import hashlib
import random
import http
import http.client
import json
import pandas as pd
from datetime import timedelta

# 转换结束时间和时长
def convert_time(seconds, excel=True):
    # 将秒数转换为 hh:mm:ss 格式
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours}:{minutes:02}:{seconds:02}"


def pause_execution():
    input("Press Enter to continue...")

def replace_chn_num(text):
    chn_num = {"一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
               "六": "6", "七": "7", "八": "8", "九": "9", "十": "10"}
    
    # 使用正则表达式进行替换
    for chn, num in chn_num.items():
        text = re.sub(chn, num, text)
    
    return text

def convert_tz(date_str, time_str, hms=True):
    # 移除时间字符串中的冒号
    temp = int(time_str.replace(":", ""))
    
    if hms:
        # 提取秒、分钟、小时
        ss = temp % 100
        temp = temp // 100
        mm = temp % 100
        temp = temp // 100
        extra_days = temp // 24
        hh = temp % 24
        
        # 创建日期时间字符串
        datetime_str = f"{date_str} {hh:02d}:{mm:02d}:{ss:02d}"
        input_datetime = pd.to_datetime(datetime_str, format="%Y-%m-%d %H:%M:%S")
    else:
        # 提取分钟、小时（无秒）
        mm = temp % 100
        temp = temp // 100
        extra_days = temp // 24
        hh = temp % 24
        
        # 创建日期时间字符串
        datetime_str = f"{date_str} {hh:02d}:{mm:02d}"
        input_datetime = pd.to_datetime(datetime_str, format="%Y-%m-%d %H:%M")

    # 加上额外的天数
    input_datetime += timedelta(days=extra_days)
    
    return input_datetime


def get_seconds(time_str, durr = True):
    # 将 hh:mm:ss 转换为秒数
    h, m, s = map(int, time_str.split(':'))
    ss = h * 3600 + m * 60 + s
    
    if not durr:
        if ss < 2*3600 + 5*60:
            ss += 24*3600
    return ss

def convert_to_excel_time(time_str):
    time_parts = [int(part) for part in time_str.split(":")]
    total_seconds = time_parts[0] * 3600 + time_parts[1] * 60 + (time_parts[2] if len(time_parts) == 3 else 0)
    return total_seconds / (24*3600)

def filter_out_empty(df, league):
    total_soccers = df[df['League'] == 'Total Soccer'].index
    keep = []
    for i in range(1, len(total_soccers)):
        not_empty = any(df.loc[j, 'League'] == league for j in range(total_soccers[i-1], total_soccers[i]))
        keep.append(not_empty)
    keep.append(any(df.loc[k, 'League'] == league for k in range(total_soccers[-1], len(df))))
    return df.drop(total_soccers[~pd.Series(keep)].index)


def baiduTranslate(translate_text, flag=1):
    '''
    :param translate_text: 待翻译的句子，len(q)<2000
    :param flag: 1:原句子翻译成英文；0:原句子翻译成中文
    :return: 返回翻译结果。
    For example:
    q=我今天好开心啊！
    result = {'from': 'zh', 'to': 'en', 'trans_result': [{'src': '我今天好开心啊！', 'dst': "I'm so happy today!"}]}
    '''

    appid = '20241025002185124'  # 填写你的appid
    secretKey = 'iGK663jutQgogPSXepQ1'  # 填写你的密钥
    httpClient = None
    myurl = '/api/trans/vip/translate'  # 通用翻译API HTTP地址
    fromLang = 'auto'  # 原文语种

    if flag:
        toLang = 'en'  # 译文语种
    else:
        toLang = 'zh'  # 译文语种

    salt = random.randint(3276, 65536)

    sign = appid + translate_text + str(salt) + secretKey
    sign = hashlib.md5(sign.encode()).hexdigest()
    myurl = myurl + '?appid=' + appid + '&q=' + urllib.parse.quote(translate_text) + '&from=' + fromLang + \
            '&to=' + toLang + '&salt=' + str(salt) + '&sign=' + sign

    # 建立会话，返回结果
    try:
        httpClient = http.client.HTTPConnection('api.fanyi.baidu.com')
        httpClient.request('GET', myurl)
        # response是HTTPResponse对象
        response = httpClient.getresponse()
        result_all = response.read().decode("utf-8")
        result = json.loads(result_all)

        # return result
        return result['trans_result'][0]['dst']

    except Exception as e:
        print(f"Translation failed: {e}")
    finally:
        if httpClient:
            httpClient.close()

