from datetime import datetime
import json
from config import STATES, CA_PROV
import pandas as pd

with open('assets/outage_type_dict.json', 'r') as file:
    OUTAGE_TYPES = json.load(file)

class outageProcessor:

    def __init__(self, csv_data):
        self.df = pd.read_csv(csv_data)
    
    def clean_edge_cases(self, indx, col, val):
        self.df.at[indx, col] = val
        return self.df

    def _to_time(self, date, time):
        return datetime.strptime(date + ' ' + self._format_time(time), '%m/%d/%Y %H:%M')

    def _format_time(self, time):
        time_data = time.split(' ')
        res = time_data[0]
        if time_data[-1].lower() in ['pm', 'p.m.']:
            res_ = res.split(':')
            if not'12' in res:
                res = ':'.join([str(int(res_[0]) + 12), res_[-1]])
            elif len(res_) > 2:
                res = ':'.join([res_[0], res_[1]])
        if len(res) < 3:
            res = res + ':00'
        return res

    def _calc_dur(self, start_date, start_time, end_date, end_time):
        if start_time == "evening":
            start_time = '17:00'

        elif not start_time:
            start_time = '10:00'

        if not end_time:
            end_time = '17:00'
        try:
            start = self._to_time(start_date, start_time)
            end = self._to_time(end_date, end_time)
            td = end - start
            res = td.days*24 + (td.seconds/3600)
        except:
            res = ''
        return res
    
    def calc_dur(self, df):
        df['duration'] = df.apply(lambda x: self._calc_dur(x['Date Event Began'], x['Time Event Began'], x['Date of Restoration'], x['Time of Restoration']), axis=1)
        return df
    
    def clean_geo_area(self, df):
        df['Geographic Areas'].fillna('', inplace=True)
        df['Geographic Areas'] = df['Geographic Areas'].apply(lambda x: x.split(' '))
        df = df.explode('Geographic Areas')
        return df
    
    def clean_states(self, df):
        df['state'] = df.apply(lambda x: x['Geographic Areas'].split(' ')[-1] if (x['Geographic Areas'].split(' ')[-1] in STATES.values() or x['Geographic Areas'].split(' ')[-1] in CA_PROV.values()) and not x['Geographic Areas'] == 'Puerto Rico' else '', axis=1)
        return df
    
    def set_outage_type(self, df):
        df['outage_type'] = df['Event Description'].apply(lambda x:OUTAGE_TYPES.get(x))
        return df
    
    def drop_negative_durs(self, df):
        negatives = list()
        for n in df.index:
            try:
                if df.loc[n, 'duration'] < 0:
                    negatives.append(n)
            except:
                continue
        df = df.drop(negatives)
        return df
    
