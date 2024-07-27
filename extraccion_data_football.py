import pandas as pd
import time
import os
from datetime import datetime

'''
url    = ['https://www.espn.com.mx/futbol/posiciones/_/liga/esp.1/temporada/2023',
         'https://www.espn.com.mx/futbol/posiciones/_/liga/eng.1/temporada/2023',
         'https://www.espn.com.mx/futbol/posiciones/_/liga/ita.1/temporada/2023',
         'https://www.espn.com.mx/futbol/posiciones/_/liga/ger.1/temporada/2023',
         'https://www.espn.com.mx/futbol/posiciones/_/liga/fra.1/temporada/2023',
         'https://www.espn.com.mx/futbol/posiciones/_/liga/por.1/temporada/2023',
         'https://www.espn.com.mx/futbol/posiciones/_/liga/ned.1/temporada/2023']

ligas = ['ESPAÑA','INGLATERRA','ITALIA','ALEMANIA','FRANCIA','PORTUGAL','HOLANDA']

df_ligas = {'LIGA':ligas,'URL':url}
df_ligas = pd.DataFrame(df_ligas)

df_espana = pd.read_html(df_ligas['URL'][0])



df_espana = pd.concat([df_espana[0],df_espana[1]],ignore_index=True,axis=1)
df_espana.rename(columns={0:'EQUIPO',1:'J',2:'G',3:'E',4:'P',5:'GF',6:'GC',7:'DIF',8:'PTS'})
df_espana[0] = df_espana[0].apply(lambda x: x[5:] if x[:2].isnumeric()==True else x[4:])
df_espana['LIGA'] = 'ESPAÑA'

print(df_espana)
'''

def get_data(url,liga):
    df_pais = pd.read_html(url)
    fec_book = datetime.now()
    df_pais = pd.concat([df_pais[0],df_pais[1]],ignore_index=True,axis=1)
    df_pais[0] = df_pais[0].apply(lambda x: x[5:] if x[:2].isnumeric()==True else x[4:])
    df_pais['LIGA'] = liga
    df_pais['FEC_BOOK'] = fec_book.strftime("%Y-%m-%d")
    return df_pais

def data_processing(ligas_df,equipos_id):
    df_spain=get_data(ligas_df['URL'][0],ligas_df['LIGA'][0])
    df_ingla=get_data(ligas_df['URL'][1],ligas_df['LIGA'][1])
    df_italy=get_data(ligas_df['URL'][2],ligas_df['LIGA'][2])
    df_germany=get_data(ligas_df['URL'][3],ligas_df['LIGA'][3])
    df_francia=get_data(ligas_df['URL'][4],ligas_df['LIGA'][4])
    df_portugal=get_data(ligas_df['URL'][5],ligas_df['LIGA'][5])
    df_holanda=get_data(ligas_df['URL'][6],ligas_df['LIGA'][6])
    df_final = pd.concat([df_spain,df_ingla,df_italy,df_germany,df_francia,df_portugal,df_holanda],ignore_index=False)
    df_final.rename(columns={0:'EQUIPO',1:'J',2:'G',3:'E',4:'P',5:'GF',6:'GC',7:'DIF',8:'PTS'},inplace=True)
    df_final = pd.merge(df_final,equipos_id,how='inner',on='EQUIPO')
    df_final = df_final[['ID_TEAM','EQUIPO','J','G','E','P','GF','GC','DIF','PTS','LIGA','FEC_BOOK']]
    df_final.to_csv("./premier_positions.csv",index=False)
    return df_final






    
