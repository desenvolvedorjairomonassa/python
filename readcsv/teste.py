from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta    
import sys
import os
import pandas as pd
import logging

csv_files = []  # Exemplo de lista

if not csv_files:
    print("A lista csv_files está vazia.")
else:
    print("A lista csv_files não está vazia.")

current_date = datetime.now()
# Caminho para o arquivo CSV
csv_file_path = 'load.csv'
sys.path.append(
    r"C:\src\dags"
)
diretorio = os.path.dirname(__file__)
print(diretorio)
caminho_completo = os.path.join(diretorio, csv_file_path)
# Leitura do arquivo CSV
df = pd.read_csv(caminho_completo)
# Exibir as primeiras linhas do DataFrame
#print(df.head())
for _, row in df.iterrows():
    print('table_name:', row['table_name'])
    print('Schedule:', row['Schedule'])
    print('max_reference_date:',row['max_reference_date'])    
    schedule = row['Schedule']
    max_date = row['max_reference_date']
    if max_date:
      if isinstance(max_date, str):
            month_max_date = datetime.strptime(max_date, '%Y-%m-%d').month
            day_max_date = datetime.strptime(max_date, '%Y-%m-%d').day
      else:
            month_max_date = max_date.month
            day_max_date = max_date.day
      print('month_max_date:', month_max_date)
      print('day_max_date', day_max_date)
      current_date = datetime.now()
      # Subtrair um mês da data atual e ajustar para o primeiro dia do mês
      previous_date = current_date - relativedelta(months=1)
      yesterday = current_date - timedelta(days=1)
      last_occurrence = datetime.now()
      # Extrair o dia do mês do Schedule, exemplo de como vem '0 08 11 * *'
      schedule_parts = schedule.split()
      # schedule_parts 0 - minuto, 1 - hora, 2 - dia do mês, 3 - mês, 4 - dia da semana
      if (len(schedule_parts) > 2) and (schedule_parts[2] != '*' and schedule_parts[4]=='*'):
            scheduled_day = int(schedule_parts[2])
      elif schedule_parts[4]!='*':
      # coloquei mais um porque no datetime, segunda começa com 0
            while last_occurrence.weekday()+1 != int(schedule_parts[4]):
                  last_occurrence -= timedelta(days=1)
            scheduled_day = -1
      else:
            scheduled_day = 0    
      print('schedule_day',scheduled_day)
    if ((current_date.day > scheduled_day and current_date.month == month_max_date and day_max_date>=scheduled_day and scheduled_day>0) or  #2
      (current_date.day < scheduled_day and previous_date.month == month_max_date and day_max_date>=scheduled_day and scheduled_day>0) or        #2.1
      (scheduled_day==0 and current_date.month == month_max_date and yesterday.day <= day_max_date) or      #1
      (last_occurrence.day == day_max_date and last_occurrence.month == month_max_date and scheduled_day ==-1)):  #3
      status = 'OK'
    else:
      status = 'Pendente'
    print(status)  
    ## break

