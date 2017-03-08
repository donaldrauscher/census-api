import requests, json, yaml, csv
import pandas as pd
import numpy as np

url = "http://api.census.gov/data/2010/sf1"

# pull in api key
with open('key.yaml', 'r') as f:
	key = yaml.load(f)

key = key['key']

# variables to pull
variables = {'P0010001':'population'}

# convert json results into pandas df
def to_pd(j):
	obj = json.loads(j)
	header = obj.pop(0)
	df = pd.read_json(json.dumps(obj), orient='values')
	df.columns = header
	return df

# download states
states = requests.get(url, params={'get':','.join([k for k,v in variables.items()]), 'for':'state:*', 'key':key})
states = to_pd(states.text)

# download zips for each state
state_codes = states.state.tolist()
zip_data = list()

for sc in state_codes:
	temp = requests.get(url, params={'get':','.join([k for k,v in variables.items()]), 'for':'zip code tabulation area:*', 'in': 'state:%s' % str(sc).zfill(2), 'key':key})
	temp_df = to_pd(temp.text)
	zip_data.append(temp_df)

zip_data = pd.concat(zip_data)

# do some formatting
zip_data.rename(columns={**variables,'zip code tabulation area':'zip'}, inplace=True)
zip_data = zip_data[['zip', 'population']]
zip_data['zip'] = zip_data.zip.apply(lambda x: str(x).zfill(5))

# export
zip_data.to_csv('./outputs/population.csv', index=False, quoting=csv.QUOTE_ALL)
