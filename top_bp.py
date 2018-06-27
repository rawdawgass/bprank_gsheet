import subprocess, os, json
import platform
import pygsheets
import pandas as pd
from pandas.io.json import json_normalize
import datetime
#bp json
votes_json = 'votes.json'

#get_json = "cleos -u http://api.proxy1a.sheos.org system listproducers -j  > " + votes_json
get_json = ("curl 'http://node2.liquideos.com/v1/chain/get_table_rows' --data-binary " + 
"'" + '{"json":true,"scope":"eosio","code":"eosio","table":"producers","limit":500}' + "'"  + ' >'  + votes_json)

#print (get_json)

#gsheet stuff
gsheet_creds = 'compelling-moon-208402-64fa16b24e5a.json'
sheet_id = '1D4C_MXaD6n8opKXifaJ47v9HYDj6icnnMFRr19GAOog'
sheet_name = 'BP Vote Extract'

def execute_bash(command):
    is32bit = (platform.architecture()[0] == '32bit')
    system32 = os.path.join(os.environ['SystemRoot'], 
                            'SysNative' if is32bit else 'System32')
    bash = os.path.join(system32, 'bash.exe')
    subprocess.call('''"%s" -c "{}"'''.format(command) % bash)


def extract_votes():
    print ('extracting votes...')
    #execute_bash(get_json)
    with open(votes_json) as f:
        data = json.load(f)
    #print (data)
    vote_df = json_normalize(data,[['rows']])
    
    vote_df['total_votes'] = vote_df['total_votes'].astype('float')

    def fix_timestamp(row):
        if int(row['last_claim_time']) > 0:
            claim_time = int(row['last_claim_time'])/1000.0
            claim_time = datetime.datetime.fromtimestamp(claim_time/1000.0)
            return claim_time

    vote_df['last_claim_time'] = vote_df.apply(fix_timestamp, axis=1)
    vote_df['rank'] = vote_df['total_votes'].rank(ascending=0)
    
    #only room for 5 columns
    vote_cols = ['rank', 'owner', 
                'last_claim_time',# 'total_producer_vote_weight'
                'total_votes', 
                #'url', 
                'vote_%']

    vote_df['last_claim_time'] = vote_df['last_claim_time'].dt.strftime('%Y-%m-%d %r')

    #% of votes
    total_votes = vote_df['total_votes'].sum()
    vote_df['vote_%'] = vote_df['total_votes'].divide(total_votes)


    vote_df = vote_df[vote_cols].sort_values(by='rank')

    #vote_df.to_csv('votes.csv', index=False)
    print ('votes extracted!')
    #print (vote_df)
    return vote_df


def update_gsheet():
    print ('Updating Gsheet')
    gc = pygsheets.authorize(service_file=gsheet_creds)
    df = extract_votes()
    #df = pd.read_csv('votes.csv')
    sh = gc.open_by_key(sheet_id)
    wks = sh.worksheet_by_title(sheet_name)
    wks.set_dataframe(df,(1,1))
    print ('Gsheet updated!')

update_gsheet()