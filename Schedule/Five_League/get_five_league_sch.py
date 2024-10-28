import requests
from bs4 import BeautifulSoup
import pandas as pd
import argparse
from openpyxl import load_workbook
from datetime import datetime
from tqdm import tqdm
import os

# 获取当前脚本文件的目录
script_dir = os.path.dirname(os.path.abspath(__file__))
sch_five_league_path = os.path.join(script_dir, 'sch_five_league.xlsx')

columns = ['League', 'Round', 'Date', 'Time', 'Home Team', 'Away Team']
df = pd.DataFrame(columns=columns)

df_map = pd.DataFrame({
    'Country': ['England', 'Spain', 'Germany', 'France', 'Italy'],
    'League': ['eng-premier-league', 'esp-primera-division', 'bundesliga', 'fra-ligue-1', 'ita-serie-a'],
    'League_Name': ['EPL', 'La Liga', 'Bundesliga', 'Ligue 1', 'Serie A']
})

def scrape_matches(country_num, season, round_num):
    global df
    url = f'https://chn.worldfootball.net/schedule/{df_map.League[country_num]}-{season}-spieltag/{round_num}/'
    response = requests.get(url)
    if response.status_code != 200:
        return
    soup = BeautifulSoup(response.content, 'html.parser')
    try:
        match = soup.find_all('table', class_='standard_tabelle')[0]
    except IndexError:
        return
    
    temp_data = []
    rows = match.find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 5:
            match_date = cells[0].text.strip()
            match_time = cells[1].text.strip()
            home_team = cells[2].text.strip()
            away_team = cells[4].text.strip()
            temp_data.append({
                'League': df_map.League_Name[country_num],
                'Round': f'Round {round_num:02d}',
                'Date': match_date,
                'Time': match_time,
                'Home Team': home_team,
                'Away Team': away_team
            })
    temp_df = pd.DataFrame(temp_data)
    df = pd.concat([df, temp_df], ignore_index=True)
    return df

parser = argparse.ArgumentParser(description='Crawl football matches data.')
parser.add_argument('season', type=str, nargs='?', default='2024-2025')
parser.add_argument('round_num_start', type=int)
parser.add_argument('round_num_end', type=int)

args = parser.parse_args()

season = args.season
k1 = args.round_num_start
k2 = args.round_num_end

for cc in tqdm(range(5), desc=f"Crawling schedule", unit="league"):
    for round_num in tqdm(range(k1, k2+1), desc=f"Crawling schedule for {df_map.League_Name[cc]}", unit="round"):
        # print(f"Scraping data for Round {round_num} in {df_map.League_Name[cc]}...")
        scrape_matches(cc, season, round_num)

df['Date'] = df['Date'].replace('', None)
df['Date'] = df['Date'].ffill()
df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce')

today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))
# df_filtered = df[df['Date'] <= today]

df.to_excel(sch_five_league_path, index=False)
# df_filtered.to_excel("D:/wangxiaoyang/Regular_Work/Schedule/Five_League/sch_five_league.xlsx", index=False)
# print('Schedule wrote to D:/wangxiaoyang/Regular_Work/Reports/Schedule/Five_League/sch_five_league.xlsx.')
