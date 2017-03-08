import requests, json, yaml, csv
import pandas as pd
import numpy as np

url = "http://api.census.gov/data/2010/sf1"

# pull in api key
with open('key.yaml', 'r') as f:
	key = yaml.load(f)

key = key['key']

# convert json results into pandas df
def to_pd(j):
	obj = json.loads(j)
	header = obj.pop(0)
	df = pd.read_json(json.dumps(obj), orient='values')
	df.columns = header
	return df

# download states
states = requests.get(url, params={'get':'P0010001,NAME', 'for':'state:*', 'key':key})
states = to_pd(states.text)

# download zips for each state
state_codes = states.state.tolist()
zip_pop = list()

for sc in state_codes:
	temp = requests.get(url, params={'get':'P0010001', 'for':'zip code tabulation area:*', 'in': 'state:%s' % str(sc).zfill(2), 'key':key})
	temp_df = to_pd(temp.text)
	zip_pop.append(temp_df)

zip_pop = pd.concat(zip_pop)

# do some formatting
zip_pop = zip_pop.rename(columns={'P0010001':'population', 'zip code tabulation area':'zip'})[['zip', 'population']]
zip_pop['zip'] = zip_pop.zip.apply(lambda x: str(x).zfill(5))

# export
zip_pop.to_csv('./zip_pop.csv', index=False, quoting=csv.QUOTE_ALL)
