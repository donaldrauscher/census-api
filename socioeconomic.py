import requests, json, yaml, csv
import pandas as pd
import numpy as np

url = "http://api.census.gov/data/2015/acs5"

# pull in api key
with open('key.yaml', 'r') as f:
	key = yaml.load(f)

key = key['key']

# variables to pull
variables = {
	'NAME':'name',
	'B07013_003E':'renter_numerator',
	'B07013_001E':'renter_denominator',
	'B25081_002E':'mortgage_numerator',
	'B25081_001E':'mortgage_denominator',
	'B07011_001E':'median_income',
	'B07009_003E':'hs_graduate_numerator',
	'B07009_005E':'college_graduate_numerator',
	'B07009_001E':'education_denominator',
	'B25010_001E':'household_size'
}

# convert json results into pandas df
def to_pd(j):
	obj = json.loads(j)
	header = obj.pop(0)
	df = pd.read_json(json.dumps(obj), orient='values')
	df.columns = header
	return df

# download data by zip
zip_data = requests.get(url, params={'get':','.join([k for k,v in variables.items()]), 'for':'zip code tabulation area:*', 'key':key})
zip_data.raise_for_status()
zip_data = to_pd(zip_data.text)
zip_data.rename(columns={**variables,'zip code tabulation area':'zip'}, inplace=True)

# calculate variables
zip_data['renter'] = zip_data.renter_numerator/zip_data.renter_denominator
zip_data['mortgage'] = zip_data.mortgage_numerator/zip_data.mortgage_denominator
zip_data['hs_graduate'] = zip_data.hs_graduate_numerator/zip_data.education_denominator
zip_data['college_graduate'] = zip_data.college_graduate_numerator/zip_data.education_denominator

# null out variables without enough sample
min_sample = 1000
zip_data.loc[zip_data.renter_denominator < min_sample, 'renter'] = None
zip_data.loc[zip_data.mortgage_denominator < min_sample, 'mortgage'] = None
zip_data.loc[zip_data.education_denominator < min_sample, ['hs_graduate','college_graduate']] = None

# export
zip_data['zip'] = zip_data.zip.apply(lambda x: str(x).zfill(5))
zip_data = zip_data[['zip','renter','mortgage','hs_graduate','college_graduate','median_income','household_size']]
zip_data.to_csv('./outputs/socioeconomic.csv', index=False, quoting=csv.QUOTE_ALL)
