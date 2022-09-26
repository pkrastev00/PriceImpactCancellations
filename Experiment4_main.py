import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
import numpy as np
import libarchive
import os
import re
import multiprocessing as mp

from utils import utils_load,utils_msg_expand,utils_cancel_expand,utils_returns,utils_cfi_agg,utils_counts


def extract_cfi_returns(zip_file_name,path_to_save):
    freq_samples = 60
    cfi_params = {'AGE_MINS':True,
                    'WHOLE_LEVEL':False,
                    'LEVEL_DIFFERENCE_SINCE_ARRIVAL':True,
                    'QUEUE':False,
                    'age_mins':40/60,
                    'level_ratio':1,
                    'level_difference':3,
                    'queue':2,
                    'WEIGHT':'Level',
                    'NORMALISE':False,
                    'c':0.5}
    
    
    with libarchive.file_reader(zip_file_name) as e:
        for entry in e:
            # name of each file in zipped folder
            individual_file_name = str(entry)

            # only consider desired file type message/orderbook
            if 'message' not in individual_file_name:
                continue
            
            date = re.findall(r"([0-9]{4}-[0-9]{2}-[0-9]{2})", individual_file_name)[0]
            if date == '2019-01-09':
                continue
            path_to_save_day = os.path.join(path_to_save,date+'.csv')
            if os.path.exists(path_to_save_day):
                continue
            
            df_msg,df_ob,ob_dict = utils_load.load_msg_ob(zip_file_name,entry)
            utils_msg_expand.expand_df_msg(df_msg,ob_dict)
            
            df_cancels = utils_cancel_expand.expand_df_cancel(df_msg, QUEUE = False)
            
            df_cancels_filtered = utils_cfi_agg.filter_df_cancels(df_cancels,cfi_params)
            #print(date)
            
            # To save
            returns = utils_returns.get_rets_all_h(df_msg,freq_samples)
            cfi_agg = utils_cfi_agg.extract_cfi_agg_day(df_msg,df_ob,df_cancels_filtered,freq_samples)
            cfi = pd.DataFrame(cfi_agg,columns=['CFI_AG'])
            counts_volume = utils_counts.get_counts_volume(df_msg)
            
            df_to_save = pd.concat([cfi,returns,counts_volume],axis=1)
            df_to_save.to_csv(path_to_save_day)

           
            

def parallel_fun(params):
    
    # Set up folder for each ticker during each year
    ticker = params[0]
    year = params[1]
    
    path_to_save_all = os.path.join('/data/cholgpu01/not-backed-up/scratch/pakraste/','4_Main_Results_normaliser')
    path_to_save_ticker_year = os.path.join(path_to_save_all,ticker+'_'+str(year)+'/')
    
    try:
        os.mkdir(path_to_save_ticker_year)
        print("Directory " , path_to_save_ticker_year ,  " Created ") 
    except FileExistsError:
        print("Directory " , path_to_save_ticker_year ,  " already exists")
    

    start_date = year + '-01-01'
    end_date = str(year) + '-12-31'
    zip_file_base_name = '_data_dwn_32_302_'
    levels = '10'
    zip_file_name = zip_file_base_name + '_' + ticker + '_' + start_date + '_' + end_date + '_' + levels + '.7z'

    # LOBSTER data
    lobster_path = '/data/cholgpu01/not-backed-up/scratch/LOBSTER/'
    # LOBSTER data for particular ticker for particular year
    ticker_path = os.path.join(lobster_path,zip_file_name)

    # make sure that there is LOBSTER data for that year for selected ticker
    if os.path.exists(ticker_path):
        extract_cfi_returns(ticker_path,path_to_save_ticker_year)

    


path_to_save = os.path.join('/data/cholgpu01/not-backed-up/scratch/pakraste/','4_Main_Results_normaliser') 
try:
    os.mkdir(path_to_save)
    print("Directory " , path_to_save ,  " Created ") 
except FileExistsError:
    print("Directory " , path_to_save ,  " already exists")
        
# File with tickers        
path_tickers = '/data/cholgpu01/not-backed-up/scratch/pakraste/PriceImpactCancel/snp500_top100_end_2019.csv'
tickers =  pd.read_csv(path_tickers,index_col=0).Ticker
years = ['2017','2018','2019']

params = []
for i in tickers:
    for j in years:
        params.append([i,j])
 
n_cpus = int(os.environ['SLURM_CPUS_PER_TASK'])

p = mp.Pool(n_cpus) 
p.map(parallel_fun, params)
