from .get_config import generate_credential
from uplink_python.uplink import Uplink
import pandas as pd


def retrieve_project():
    cred = generate_credential()
    sat = cred["satellite"]
    api = cred["api_key":]
    phrase = cred["passphrase"]
    bucket_name = cred["bucket_name"]

    uplink = Uplink()
    access = uplink.request_access_with_passphrase(sat, api, phrase)
    project = access.open_project()

    return project, bucket_name


def read_file(coin, project, bucket_name, file_name, size_to_read=256): 
    download = project.download_object(bucket_name, file_name)
    file_size = download.file_size()
    downloaded_total = 0
    all_data = ''
    while True:
        # read from Storj bucket
        data_read, bytes_read = download.read(size_to_read)
        # decode data
        all_data += data_read.decode("utf-8")  
        # update last read location
        downloaded_total += bytes_read
        # break if download complete
        if downloaded_total == file_size:
            break 
    return all_data


def convert_to_df(raw_data):
    all_rows = raw_data.split('\n')
    col_name = all_rows[0].split(',')
    all_extract = []
    for row in all_rows[1:]:
        extract = row.split(',')
        all_extract.append(extract)
    df = pd.DataFrame(all_extract, columns=col_name)  
    return df


def read_rawdata(ticker_list):
    project, bucket_name = retrieve_project()    
    all_df_raw = {} 
    for coin in ticker_list:  
        file_name = f'{coin.lower()}-usd-max.csv' 
        all_data = read_file(coin, project, bucket_name, file_name)
        df = convert_to_df(all_data)
        all_df_raw.update({coin:df})
    return all_df_raw