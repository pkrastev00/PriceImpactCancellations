# -*- coding: utf-8 -*-
"""
@author: panch
"""

import numpy as np
import pandas as pd

def get_cfi_level(df_cancels,level):
        
    df_cancels_level = df_cancels[df_cancels.Level == level]
    cancel_flow_bid = df_cancels_level[ (df_cancels_level.TradeDirection == 1)].Size.sum()
    cancel_flow_ask = df_cancels_level[ (df_cancels_level.TradeDirection == -1)].Size.sum()
    
    cfi = cancel_flow_bid - cancel_flow_ask

    return cfi

def get_cfi(df_msg_period,df_cancels,cfi_agg_params):
    
    AGE_MINS = cfi_agg_params['AGE_MINS']
    WHOLE_LEVEL = cfi_agg_params['WHOLE_LEVEL']
    LEVEL_DIFFERENCE_SINCE_ARRIVAL = cfi_agg_params['LEVEL_DIFFERENCE_SINCE_ARRIVAL']
    QUEUE = cfi_agg_params['QUEUE']
    
    age_mins = cfi_agg_params['age_mins']
    level_ratio = cfi_agg_params['level_ratio']
    level_difference = cfi_agg_params['level_difference']
    queue = cfi_agg_params['queue']
    
    df_cancels_period = df_cancels[df_cancels.index.isin(df_msg_period.index)]
    df_cancels_period_filtered = df_cancels_period.copy()
    
    # Filtering
    if QUEUE:
        df_cancels_period_filtered = df_cancels_period_filtered[df_cancels_period_filtered.Queue <= queue]
    if AGE_MINS:
        df_cancels_period_filtered = df_cancels_period_filtered[(df_cancels_period_filtered.AgeMins > age_mins)]
    if WHOLE_LEVEL and LEVEL_DIFFERENCE_SINCE_ARRIVAL:
        df_cancels_period_filtered = df_cancels_period_filtered[(df_cancels_period_filtered.Size/df_cancels_period_filtered.LevelSize >= level_ratio) | (np.abs(df_cancels_period_filtered.Level - df_cancels_period_filtered.LevelArrival) >= level_difference)]
    if WHOLE_LEVEL and not LEVEL_DIFFERENCE_SINCE_ARRIVAL:
        df_cancels_period_filtered = df_cancels_period_filtered[(df_cancels_period_filtered.Size/df_cancels_period_filtered.LevelSize >= level_ratio)]
    if LEVEL_DIFFERENCE_SINCE_ARRIVAL and not WHOLE_LEVEL:
        df_cancels_period_filtered = df_cancels_period_filtered[ df_cancels_period_filtered.Level - df_cancels_period_filtered.LevelArrival < level_difference]
    
    
    cfi = np.zeros((10))
    
    for level in range(1,11):
        cfi[level-1] = get_cfi_level(df_cancels_period_filtered,level)

    return pd.Series(cfi,index=range(1,11)) #pd.DataFrame(cfi,columns=[i for i in range(1,11)])

def get_CFI(df_msg,df_cancels,freq_samples,cfi_agg_params):
    df_msg_keep = df_msg[(df_msg.Time >= 10*3600)&(df_msg.Time <= 15.5*3600)]
    start_group = int(10*3600 // freq_samples)
    end_group = int(15.5*3600 // freq_samples)
    df_msg_period = df_msg_keep.groupby(df_msg_keep.Time // freq_samples)
    return df_msg_period.apply(get_cfi,df_cancels = df_cancels,cfi_agg_params=cfi_agg_params).reindex(range(start_group,end_group)).fillna(0).reset_index(drop=True)
