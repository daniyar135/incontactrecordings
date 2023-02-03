# Consolidate & Evolve Work-to-Date
# Icebreaker.py
#Dale Weber, Daniyar Sadykhanov
# %% Import
import fcntl
from re import A
from urlgrabber.grabber import URLGrabber
import matplotlib.dates as mdates
import requests
import base64
import datetime as DT
import pandas as pd
import os
from os import listdir
import numpy as np
#%% Prep0: Authorization
# https://developer.niceincontact.com/Documentation/BackendApplicationAuthentication

def nice_auth():
    url_token_viakey = 'https://api.incontact.com/InContactAuthorizationServer/token/access-key'

    APIUserKey = ''
    APIUser_Secret = ''

    auth_payload = {
        'accessKeyId': APIUserKey,
        'accessKeySecret': APIUser_Secret
    }

    head = {'content-type': 'application/json'}
    r_token_viakey = requests.post(
        url=url_token_viakey, headers=head, json=auth_payload)
    # Retrieve Access Token
    atoken = r_token_viakey.json()['access_token']
    # Update head w/token
    head['Authorization'] = 'Bearer {}'.format(atoken)
    return head


#%% Prep1: Top 10 Tech Skills
skills = {
    'NA Sunvault Tech': 10519755,
    #'NA Master - TSE': 4305607,
    #'NA TSE': 4305606,
    #'NA CSR Technical':4305582,
    #'NA Commercial TSE': 4305602,
    #'NA TSE Indirect Commercial': 4305603,
    #'NA Escalations': 4305590,
    #'Commercial Commissioning':10463443,
    #'NA NH TSE':4313845,
    #'NA CSR Tech 2':10796840
}

#%% Prep2: Date Ranges
startDate = DT.date.today() - DT.timedelta(days=20)
endDate = DT.date.today() - DT.timedelta(days=1)
print("get recordings from ", str(startDate), " to ", str(endDate))
week0 = mdates.num2date(mdates.drange(startDate,endDate,DT.timedelta(hours=6)))
wk = {'0':{'st':[],'end':[]},'1':{'st':[],'end':[]}}
i=0
while i+1<len(week0):
    wk['0']['st'].append(str(week0[i]))
    wk['0']['end'].append(str(week0[i+1]+DT.timedelta(seconds=-1)))
    wk['1']['st'].append(str(week0[i]-DT.timedelta(7)))
    wk['1']['end'].append(str(week0[i+1]-DT.timedelta(7)+DT.timedelta(seconds=-1)))
    i+=1
# %% Pull Data
ct = 0
c = []
#
url_cluster = 'https://api-c29.incontact.com/inContactAPI/services/v23.0/'
api_nm = 'contacts/completed'
head = nice_auth()
result = []
for skill in skills:
    for k in wk:
        j = 0
        while j < i:
            query_contacts = {
                #'updatedSince': '2022-01-01',
                'startDate': wk[k]['st'][j],
                'endDate': wk[k]['end'][j],
                'skillId': skills[skill],
                'mediaTypeId': 4,
                'fields': 'isOutbound, contactStart, totalDurationSeconds,contactId, fromAddr,mediaType,mediaTypeName,skillId,skillName,campaignId,campaignName,teamId,teamName',
            }
            j += 1
            r = requests.get(url=url_cluster+api_nm, headers=head, params=query_contacts)
            if r.status_code != requests.codes.ok:
                continue
            else:
                result.append(r.json()['completedContacts'])
                c.append(r.json()['totalRecords'])
                ct += r.json()['totalRecords']
                print('{}: {} - {}'.format(skill,query_contacts['startDate'],query_contacts['endDate']))
                print(ct)
         
#%% Read data from result list
con = []
for res in result:
    for cc in res:
        con.append(cc)
df = pd.DataFrame.from_records(con, columns=list(cc.keys()))
df['totalDurationSeconds']=df['totalDurationSeconds'].replace(',', '', regex=True).astype(float)
df['totalDurationSeconds'] = pd.to_numeric(df['totalDurationSeconds'], errors='raise')
for x in df.index:
    df.loc[x,'aCode'] = df.loc[x,'fromAddr'][-10:][:3]
    df.loc[x,'totalDurationSeconds']=df.loc[x,'totalDurationSeconds']/60.0

df.rename(columns={'totalDurationSeconds':'totalDurationMinutes'}, inplace=True)
df['LongTF']=df['totalDurationMinutes']>=120
df['contactStart']=pd.to_datetime(df['contactStart'])
df.set_index('contactStart', inplace=True)
df['wk'] = df.index.isocalendar().week
df['day'] = df.index.isocalendar().day
df['hour'] = df.index.hour
#df_desc_stats = df.groupby(['skillName', 'wk', 'LongTF'])['totalDurationMinutes'].describe()
#print(df_desc_stats)
# %% Longest Calls (IDs)
long5_df = df[['contactId','totalDurationMinutes','wk']][(df['wk']==df['wk'].max())].sort_values(by='totalDurationMinutes', ascending=False).head(5)
#%% Prep to Download Calls
for index, row in long5_df.iterrows():
    api_nm_files = 'contacts/{}/files'.format(row['contactId'])
    print(row['contactId'])
    r_file = requests.get(url=url_cluster+api_nm_files, headers=head)
    long5_df.loc[index,'file_url'] = r_file.json()['files'][0]['fullFileName']
    long5_df.loc[index,'file_name'] = r_file.json()['files'][0]['fileName']
    
# %% Download Calls
api_nm_bin = 'files'
audioPath = "C://Users/{}/OneDrive - Sunpower Corporation/ATSE.US - Documents/Icebreaker/TestLogs/".format(os.getlogin())
mode = 0o666

for index_bin, row_bin in long5_df.iterrows():
    head = nice_auth()
    g = URLGrabber(ssl_verify_peer = False, ssl_verify_host = False, curlopt_returntransfer = False)
    ocal_filename = g.urlread(url=url_cluster + api_nm_bin + '?fileName=' + row_bin['file_url'], http_headers=tuple(head.items()))
    oocal_filename = base64.b64decode(str(ocal_filename).split(':')[2])

    print('Writing #{} ID-{}, {} minutes long'.format(i+1,row_bin['contactId'], int(row_bin['totalDurationMinutes'])))

    with open('wk{}_{}.wav'.format(row_bin['wk'],row_bin['contactId']), 'wb') as f:
        f.write(oocal_filename)
        local_filename = 'wk' + str(row_bin['wk']) + '_' + str(row_bin['contactId'])
