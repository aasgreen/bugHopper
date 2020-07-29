import json
import numpy as np
import pandas as pd
import requests
import ratelimit as rt
from datetime import date
from backoff import on_exception, expo

#class bugBase():
#    def __init__(self):


@on_exception(expo, rt.RateLimitException, max_tries = 8)
@rt.limits(calls = 10, period = 70)
def call_basic(url, user, paswd, params = None):
    got = requests.get(url, params = params, auth = (user, paswd)).content
    return got

user_joia = 'ao8isgnz4mgjpy8y2ye3dq'
paswd_joia = 'Eg%zLk1dtC85a$W'


#@on_exception(expo, rt.RateLimitException, max_tries = 8)

@rt.sleep_and_retry
@rt.limits(calls = 1, period = 1.1)
def call_joai(url, params = None):
    got = requests.get(url, params = params, auth = (user_joia, paswd_joia)).content
    return got


url_base = 'https://www.bugherd.com/api_v2/'
url = 'https://www.bugherd.com/api_v2/users/members.json'
user_test = 'eeiwx20158lg3mq5juidag'
paswd_test = 'testtesttest'
test = call_basic(url, user_test, paswd_test)


url_users = url_base + 'users.json'
url_users_guests = url_base + 'users/guests.json'

test_users = call_basic(url_users_guests, user_test, paswd_test)


url_projects = url_base + 'projects.json'
url_projects_active = url_base + 'projects/active.json'

total = json.loads(call_joai(url_projects_active, params = None))['meta']['count']

def get_big_list(end):
    dfbig = []
    for i in np.arange(np.round(end/100),np.round(end/100)+1)[::-1]:
        pg = {'page': int(i)}
        tempJson = call_joai(url_projects_active, params = pg)
        dfbig.append(pd.json_normalize(json.loads(tempJson)['projects']))
    return pd.concat(dfbig)

test_name = 'Cheri Adrian'
#cheri_row = data['name'].str.contains(test_name)

#cheri_id = data[cheri_row]['id']
cheri_id = np.array([204663])

url_tasks = url_base+'projects/'+str(cheri_id[0]) + "/tasks/feedback.json"

cheri_tasks = json.loads(call_joai(url_tasks))

#this will be ugly, clean up later:

#first, build up a dataframe of the first 200 projects

proj_dataframe = get_big_list(total)

#now, for each active project, we need to get a dataframe of tasks

task = []
for index,row in proj_dataframe[0:10].iterrows():
    url_tasks = url_base + 'projects/' + str(row['id']) + "/tasks/feedback.json"
    task_df = pd.json_normalize(json.loads(call_joai(url_tasks))['tasks'])
    if (~task_df.empty):
        task_df['name'] = row['name']
        task.append(task_df)
    print('{}% done'.format(index/len(proj_dataframe)))

df = pd.concat(task)
df['created_at'] = pd.to_datetime(df['created_at'], utc=False)
df['created_at'] =df['created_at'].dt.tz_convert('US/Mountain')
gprTask = df.groupby('name')
l = gprTask.agg(
        size=pd.NamedAgg(column='id', aggfunc='size'),
        created_at=pd.NamedAgg(column='created_at', aggfunc='min'),
        email=pd.NamedAgg(column='requester_email', aggfunc='min'))


