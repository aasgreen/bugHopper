import numpy as np
import pandas as pd
import gspread as gp
import json
import requests
import ratelimit as rt
from datetime import date
from backoff import on_exception, expo
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import yaml

#to do:
# duplicate name handling (but update pins)
# time handling (updating days, and calculating buisness hours elasped)
#sorting new so it goes from oldest to newest

class bugHopper:
    '''Object that interfaces between bugherd api and google sheets'''

    def __init__(self):
        with open('./api.yml', 'r') as file:
            api = yaml.load(file, Loader = yaml.FullLoader)

        self.user_basic = api['api']['user']
        self.paswd_basic = api['api']['pass']
        self.url_base = 'https://www.bugherd.com/api_v2/'
        self.url_projects_active = self.url_base + 'projects/active.json'
        self.project_pd = []
        self.total_proj = 100

        self.scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', self.scope)
        self.sp_name = "BugHerd COPY"

    @rt.sleep_and_retry
    @rt.limits(calls = 1, period = 2)
    def call_basic(self, url, params = None):
        got = requests.get(url, params = params, auth = (self.user_basic, self.paswd_basic)).content
        return got

    def get_total_projects(self):
        self.total_proj = json.loads(self.call_basic(self.url_projects_active))['meta']['count']
        return self.total_proj

    def get_project_list(self, end=100):
        dfbig = []
        for i in np.arange(1,np.round(end/100)+1)[::-1]:
            pg = {'page': int(i)}
            tempJson = self.call_basic(self.url_projects_active, params = pg)
            dfbig.append(pd.json_normalize(json.loads(tempJson)['projects']))
            self.project_pd = pd.concat(dfbig)
        dfbig = None
        return self.project_pd

    def get_task_list(self):

        task = []
        count = 0
        for index,row in self.project_pd.iterrows():
            url_tasks = self.url_base + 'projects/' + str(row['id']) + "/tasks/feedback.json"
            try:
                tempJson = self.call_basic(url_tasks)
                task_df = pd.json_normalize(json.loads(tempJson)['tasks'])
            except KeyError:
                print(json.loads(tempJson))
            if (~task_df.empty):
                task_df['name'] = row['name']
                task.append(task_df)
                count += 1
            print('{}% done, index:{}, total:{}'.format(count/len(self.project_pd), count, len(self.project_pd)))

        df = pd.concat(task)
        df['created_at'] = pd.to_datetime(df['created_at'], utc=False)
        df['created_at'] =df['created_at'].dt.tz_convert('US/Mountain')
        self.task_pd = df
        return self.task_pd

    def process_task_list(self):
        gprTask = self.task_pd.groupby('name')
        l = gprTask.agg(
            size=pd.NamedAgg(column='id', aggfunc='size'),
            created_at=pd.NamedAgg(column='created_at', aggfunc='min'),
            email=pd.NamedAgg(column='requester_email', aggfunc='min')).reset_index()
        #l['created_at'] = l['created_at'].dt.strftime('%m/%d/%h')
        self.task_process = l
        self.task_process.rename(columns = {'size':'# of Pins', 'email': 'Client Email', 'name':'Client'}, inplace=True)
        self.task_process['From'] = 'BugHerd'
        self.task_process['Date'] = date.today().strftime("%m/%d")
        self.task_process['Time'] = datetime.datetime.now()
        self.task_process['Time'] = self.task_process['Time'].dt.tz_localize('US/Mountain')
        self.task_process = self.task_process[self.task_process['created_at'] >'2020-01-01']

        def hours_elap(start, end):
            '''
               start and end are pandas timestamp objects
            '''
            n_hours = (end-start).days*24+(end-start).seconds/3600.0
            b_days = len(pd.bdate_range(start, end))
            if (n_hours > 24.):
                #more than a buisness day, so return number of days
                return '{} days'.format(round(b_days))
            else:
                return '{} hours'.format(round(n_hours))

        self.task_process['Last updated'] = self.task_process.apply(lambda x: hours_elap(x['created_at'], x['Time']), axis = 1)
        self.task_process.sort_values('created_at', inplace=True) 

    def callSheet(self):
        client = gp.authorize(self.creds)
        data = client.open(self.sp_name).sheet1
        return data

    def get_spreadsheet(self):
        self.sheet = self.callSheet() 
        self.prev_df = pd.DataFrame(self.sheet.get_all_records())
        self.last_row = len(self.prev_df)+1
        return self.prev_df

    def get(self):
        self.get_total_projects()
        self.get_project_list(l.total_proj)
        self.get_task_list()
        self.process_task_list()

    def combine(self):
        
        firstRun = self.prev_df[self.prev_df['Client Notified'] != 'Yes']
        todayData = firstRun[firstRun['Date'] != '']
        todayData['From'] = 'Google'
        self.big_sheet = pd.concat([todayData,self.task_process]).fillna('')
        self.big_sheet_copy = self.big_sheet[:]
        self.big_sheet.drop(['From', 'created_at', 'Time'], axis = 1, inplace= True)



    def put(self):
        update_dict_list = []
        for i, update_row in enumerate(self.big_sheet.iterrows()):
            row = i+self.last_row
            grange = r'A{}:O{}'.format(row,row)
            print(update_row)
            update_dict_list.append({'range':grange, 'values': [update_row[1].values.tolist()]})
        self.sheet.batch_update(update_dict_list)
    def write_local(self):
        self.big_sheet.to_pickle('bug.pkl')
    def write_raw(self):
        self.task_process.to_pickle('bugProc.pkl')
        self.task_pd.to_pickle('bugRaw.pkl')
    def read_local(self):
        self.big_sheet = pd.read_pickle('bug.pkl')
    def read_raw(self):
        self.task_pd = pd.read_pickle('bugRaw.pkl')
if __name__ == '__main__':
    l = bugHopper()
    #l.get()
    #l.write_raw()
    l.read_raw()
    l.process_task_list()
    l.get_spreadsheet()
    l.combine()
    l.put()
    l.write_local()
    l.read_local


