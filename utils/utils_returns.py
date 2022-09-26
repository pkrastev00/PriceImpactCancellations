# -*- coding: utf-8 -*-
"""
@author: panch
"""
import pandas as pd
import numpy as np


def rets(df_msg_period):
    if df_msg_period.shape[0] == 0:
        return 0
    start_price = df_msg_period['MidPrice'].iloc[0]
    end_price = df_msg_period['MidPrice'].iloc[-1]
    if start_price <= 0 or end_price <= 0:
        ret = 0
    else:
        ret = np.log(end_price / start_price)
    return ret

def get_rets(df_msg,freq_samples):
    df_msg_keep = df_msg[(df_msg.Time >= 10*3600)&(df_msg.Time <= 15.5*3600)]
    #start_group = int(10*3600 // freq_samples)
    #end_group = int(15.5*3600 // freq_samples)
    df_msg_period = df_msg_keep.groupby(df_msg_keep.Time // freq_samples)
    return df_msg_period.apply(rets)

def get_rets_h(df_msg,h,freq_samples=60):
    start_time = int(10*3600)
    end_time = int(15.5*3600)
    r = np.zeros((int(5.5*3600/freq_samples),))
    arr_index = 0
                 
    for i in range(start_time,end_time,freq_samples):
        start_period = i
        #end_period = i+freq_samples
        end_custom_period = i+h
        df_msg_period = df_msg[(df_msg.Time >= start_period)&(df_msg.Time < end_custom_period)]
        r[arr_index] = rets(df_msg_period)
        arr_index+=1
    return r

def get_rets_all_h(df_msg,freq_samples):
    horizons = [10,30,60,120,300]
    r = np.zeros((int(5.5*3600/freq_samples),len(horizons)))
    arr_index = 0
    for h in horizons:
        r[:,arr_index] = get_rets_h(df_msg,h,freq_samples)
        arr_index+=1
    return pd.DataFrame(r,columns=horizons)
