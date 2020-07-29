import json
import numpy as np
import pandas as pd
import requests
import ratelimit as rt
from datetime import date
from backoff import on_exception, expo


@on_exception(expo, rt.RateLimitException, max_tries = 8)
@rt.limits(calls = 10, period = 70)
def call_basic(url, user, paswd, params = None):
    got = requests.get(url, params = params, auth = (user, paswd)).content
    return got

user_joia = 'ao8isgnz4mgjpy8y2ye3dq'
paswd_joia = 'Eg%zLk1dtC85a$W'


#@on_exception(expo, rt.RateLimitException, max_tries = 8)
@rt.sleep_and_retry
@rt.limits(calls = 1, period = 2)
def call_joai(url, params = None):
    got = requests.get(url, params = params, auth = (user_joia, paswd_joia)).content
    return got


url_base = 'https://www.bugherd.com/api_v2/'
url = 'https://www.bugherd.com/api_v2/users/members.json'
user_test = 'eeiwx20158lg3mq5juidag'
paswd_test = 'testtesttest'
test = call_basic(url, user_test, paswd_test)

#playing around with api calls

#users

url_users = url_base + 'users.json'
url_users_guests = url_base + 'users/guests.json'

test_users = call_basic(url_users_guests, user_test, paswd_test)


url_projects = url_base + 'projects.json'
url_projects_active = url_base + 'projects/active.json'

#test_projects = call_basic(url_projects, user_test, paswd_test)
#test_projects_active = call_basic(url_projects_active, user_joia, paswd_joia)

#ll = json.loads(test_projects_active)

#total = ll['meta']['count']

#total_pages = total // 100

#the next thing is to call the last page

#last_page_params = {'page': total_pages + 1}
#last_page_projects = call_basic(url_projects_active, user_joia, paswd_joia, params = last_page_params)

#df  = pd.json_normalize(json.loads(last_page_projects)['projects'])

#need to build a database of all projects as bugherd organizes all the projects probably by time the project was created, not updated.

def get_big_list():
    dfbig = []
    for i in np.arange(1, np.round(total/100)+1 ):
        pg = {'page': int(i)}
        tempJson = call_joai(url_projects_active, params = pg)
        dfbig.append(pd.json_normalize(json.loads(tempJson)['projects']))
    return pd.concat(dfbig)

test_name = 'Cheri Adrian'
#cheri_row = data['name'].str.contains(test_name)

#cheri_id = data[cheri_row]['id']
cheri_id = np.array([204663])

url_tasks = url_base+'projects/'+str(cheri_id[0]) + "/tasks/feedback.json"

cheri_tasks = call_joai(url_tasks)
