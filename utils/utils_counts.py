import pandas as pd
import numpy as np

def get_counts_volume_period(df_msg_period):
    df_cancels_period = df_msg_period[(df_msg_period.Type == 2)|(df_msg_period.Type == 3)]
    #if df_cancels_period.shape[0] == 0:
        #return pd.Series([0,0,0,0])
    counts_buy = df_cancels_period[(df_cancels_period.TradeDirection == 1)].shape[0]
    counts_sell = df_cancels_period[(df_cancels_period.TradeDirection == -1)].shape[0]
    volume_buy = df_cancels_period[(df_cancels_period.TradeDirection == 1)].Size.sum()
    volume_sell = df_cancels_period[(df_cancels_period.TradeDirection == -1)].Size.sum()
    return pd.Series( [counts_buy,volume_buy,counts_sell,volume_sell] )

def get_counts_volume(df_msg,freq_samples=60):
    df_msg_keep = df_msg[(df_msg.Time >= 10*3600)&(df_msg.Time <= 15.5*3600)]
    start_group = int(10*3600 // freq_samples)
    end_group = int(15.5*3600 // freq_samples)
    df_msg_period = df_msg_keep.groupby(df_msg_keep.Time // freq_samples)
    df = df_msg_period.apply(get_counts_volume_period).reindex(range(start_group,end_group)).fillna(0).reset_index(drop=True)
    df.columns = ['BuyCounts','BuyVolume','SellCounts','SellVolume']
    return df
