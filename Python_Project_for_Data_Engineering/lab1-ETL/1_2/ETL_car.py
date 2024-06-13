import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = 'log_file.txt'
target_file = 'transformed_data.csv'

# 1. Extract
# extract from csv files
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# extract from json files
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe

# extract from xml files
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns = ['car_model', 'year_of_manufacture', 'price', 'fuel'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()

    for row in root:
        car_model = row.find('car_model').text
        year_of_manufacture = int(row.find('year_of_manufacture').text)
        price = float(row.find('price').text)
        fuel = row.find('fuel').text
        dataframe = pd.concat([dataframe, pd.DataFrame([{'car_model': car_model, 'year_of_manufacture': year_of_manufacture, 'price': price, 'fuel': fuel}])], ignore_index=True)
    return dataframe


def extract():

    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel']) # create an empty data frame to hold extracted data
    
    #process all csv files
    for csvfile in glob.glob('*.csv'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)
    
    #process all json files
    for jsonfile in glob.glob('*.json'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    #process all xml files
    for xmlfile in glob.glob('*.xml'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)

    return extracted_data

# 2. Transform
def transform(data):
    '''round the price to two decimal places'''
    data['price'] = round(data.price, 2)

    return data

# 3. Loading and Logging
def load_data(taget_file, transformed_data):
    transformed_data.to_csv(taget_file)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ',' + message + '\n')

# TESTING
# log the installation of the ETL process
log_progress('ETL job started')

# Log the beginning of the Extraction process
log_progress('Extract phase started')
extracted_data = extract()

# Log the completion of the extraction process
log_progress('Extract phase ended')

# Log the beginning of the Transformation process
log_progress('Transform phase started')

transformed_data = transform(extracted_data)
print('Tranformed Data')
print(transformed_data)

# Log the completion of the Transformation process
log_progress('Transform phase ended')

# Log the beginning of the Loading process
log_progress('Load phase started')
load_data(target_file, transformed_data)

# Log the completion of the Loading process
log_progress('Load phase ended')

# Log the completion of the ETL process 
log_progress("ETL Job Ended")