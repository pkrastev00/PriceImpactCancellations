# -*- coding: utf-8 -*-
"""
@author: panch
"""

import numpy as np

def filter_df_cancels(df_cancels,cfi_agg_params):
    AGE_MINS = cfi_agg_params['AGE_MINS']
    LEVEL_DIFFERENCE_SINCE_ARRIVAL = cfi_agg_params['LEVEL_DIFFERENCE_SINCE_ARRIVAL']
#    WHOLE_LEVEL = cfi_agg_params['WHOLE_LEVEL']
#     WEIGHT = cfi_agg_params['WEIGHT']
#     #NORMALISE = cfi_agg_params['NORMALISE']
#     QUEUE = cfi_agg_params['QUEUE']

    age_mins = cfi_agg_params['age_mins']
    level_difference = cfi_agg_params['level_difference']
#     level_ratio = cfi_agg_params['level_ratio']
#     queue = cfi_agg_params['queue']

    df_cancels_filtered = df_cancels.copy()


    # Filtering
    if AGE_MINS and LEVEL_DIFFERENCE_SINCE_ARRIVAL:
        df_cancels_filtered = df_cancels_filtered[(df_cancels_filtered.AgeMins > age_mins) | (np.abs(df_cancels_filtered.Level - df_cancels_filtered.LevelArrival) > level_difference) ]
    elif AGE_MINS and not LEVEL_DIFFERENCE_SINCE_ARRIVAL:
        df_cancels_filtered = df_cancels_filtered[(df_cancels_filtered.AgeMins > age_mins)]
    elif LEVEL_DIFFERENCE_SINCE_ARRIVAL and not AGE_MINS:
        df_cancels_filtered = df_cancels_filtered[ (np.abs(df_cancels_filtered.Level - df_cancels_filtered.LevelArrival) > level_difference) ] 
#     if QUEUE:
#         df_cancels_filtered = df_cancels_filtered[df_cancels_filtered.Queue <= queue]
#     if WHOLE_LEVEL:
#         df_cancels_filtered = df_cancels_filtered[(df_cancels_filtered.Size/df_cancels_filtered.LevelSize >= level_ratio)]
    
    return df_cancels_filtered

def get_total_volume(df_ob,level):
    idx = 4 * (level - 1)
    df_ob_level = df_ob.iloc[:,idx:(idx+4)]
    volume_level = (df_ob_level['Ask Size '+str(level)] + df_ob_level['Bid Size '+str(level)])/2
    mean_volume = volume_level.mean()
    return mean_volume

def get_normaliser(df_ob_period):
    mean_volume = np.zeros((10))
    
    for level in range(1,11):
        mean_volume[level-1] = get_total_volume(df_ob_period,level)
    normaliser = mean_volume.sum()
    return normaliser

def get_cfi_agg(df_msg_period,df_cancels,df_ob):
    
    df_cancels_period = df_cancels[df_cancels.index.isin(df_msg_period.index)]
    df_cancels_period_filtered = df_cancels_period.copy()

    w = df_cancels_period_filtered.Level
    
    cfi = (np.exp(-0.5*w)*df_cancels_period_filtered.Size*df_cancels_period_filtered.TradeDirection).sum()
    df_ob_period = df_ob[df_ob.index.isin(df_cancels_period.index)]
    normaliser = get_normaliser(df_ob_period)
    if normaliser != 0:
        return cfi/normaliser
    else:
        return cfi


def extract_cfi_agg_day(df_msg,df_ob,df_cancels,freq_samples):
    
    df_msg_keep = df_msg[(df_msg.Time >= 10*3600)&(df_msg.Time <= 15.5*3600)]
    start_group = int(10*3600 // freq_samples)
    end_group = int(15.5*3600 // freq_samples)
    
    df_msg_period = df_msg_keep.groupby(df_msg_keep.Time // freq_samples)

    # cfi aggregated
    #if cfi_agg_params['NORMALISE'] == 2:
     #   normaliser = df_msg_period.apply(get_normaliser,df_ob = df_ob).reindex(range(start_group,end_group)).fillna(0).reset_index(drop=True)
      #  cfi_agg = df_msg_period.apply(get_cfi_agg,df_cancels = df_cancels,cfi_agg_params = cfi_agg_params).reindex(range(start_group,end_group)).fillna(0).reset_index(drop=True)
       # cfi_agg = cfi_agg/normaliser
    #else:
    cfi_agg = df_msg_period.apply(get_cfi_agg,df_cancels = df_cancels,df_ob=df_ob).reindex(range(start_group,end_group)).fillna(0).reset_index(drop=True)
    
    return cfi_agg
