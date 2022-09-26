# -*- coding: utf-8 -*-
"""
@author: pancho
"""

import numpy as np

'''
def extract_cfi_top(df_cancels_expanded,freq_sample_mins = 1):
    start_time = 10*60*60
    end_time = 15.5*60*60
    freq = freq_sample_mins*60
    
    cfi = np.zeros( shape = (int((end_time - start_time)/freq),) )
    returns = np.zeros( shape = (int((end_time - start_time)/freq),) )
    arr_index = 0
    
    for i in range(int(start_time) + freq ,int(end_time) + freq,freq):
        period_start = i - freq
        period_end = i
        df_period_sample = df_cancels_expanded[ (df_cancels_expanded.Level == 1) &  (df_cancels_expanded.Time > period_start) & (df_cancels_expanded.Time <= period_end) ]
        if df_period_sample.shape[0] == 0:
                cfi[arr_index] = 0
                returns[arr_index] = 0
                continue    
        bid_volume = df_period_sample[(df_period_sample.TradeDirection == 1)].Size.sum()
        ask_volume = df_period_sample[(df_period_sample.TradeDirection == -1)].Size.sum()
        cfi[arr_index] = (bid_volume-ask_volume)/(bid_volume + ask_volume)
        returns[arr_index] = np.log(df_period_sample.MidPrice.iloc[df_period_sample.MidPrice.shape[0]-1]/df_period_sample.MidPrice.iloc[0])
        arr_index+=1
        
    return cfi,returns
'''

'''
def get_ofi_level_2(df_period_msg,df_ob,up_to_level):
    ofi = np.zeros(shape = (up_to_level,))
    for i in range(1,up_to_level+1):

        df_period_level = df_period_msg[(df_period_msg.Level == i)]

        ofi_df = df_ob[['Ask Price '+str(i), 'Ask Size '+str(i), 'Bid Price '+str(i), 'Bid Size '+str(i)]].copy().reset_index()
        ofi_df['PriceBid_prev'] = ofi_df['Bid Price '+str(i)].shift()
        ofi_df['SizeBid_prev'] = ofi_df['Bid Size '+str(i)].shift()
        ofi_df['PriceAsk_prev'] = ofi_df['Ask Price '+str(i)].shift()
        ofi_df['SizeAsk_prev'] = ofi_df['Ask Size '+str(i)].shift()
        bid_greater = ofi_df['Bid Price '+str(i)] > ofi_df['PriceBid_prev']
        bid_equal = (ofi_df['Bid Price '+str(i)] == ofi_df['PriceBid_prev'])&(ofi_df['Bid Size '+str(i)] != ofi_df['SizeBid_prev'])
        bid_less = ofi_df['Bid Price '+str(i)] < ofi_df['PriceBid_prev']

        ask_greater = ofi_df['Ask Price '+str(i)] > ofi_df['PriceAsk_prev']
        ask_equal = (ofi_df['Ask Price '+str(i)] == ofi_df['PriceAsk_prev'])&(ofi_df['Ask Size '+str(i)] != ofi_df['SizeAsk_prev'])
        ask_less = ofi_df['Ask Price '+str(i)] < ofi_df['PriceAsk_prev']

        OFI = np.zeros(len(ofi_df))
        OFI[bid_greater] = ofi_df['Bid Size '+str(i)][bid_greater]
        OFI[bid_equal] = ofi_df['Bid Size '+str(i)][bid_equal] - ofi_df['SizeBid_prev'][bid_equal]
        OFI[bid_less] = -ofi_df['SizeBid_prev'][bid_less]

        OFI[ask_greater] = ofi_df['SizeAsk_prev'][ask_greater]
        OFI[ask_equal] =  ofi_df['SizeAsk_prev'][ask_equal] - ofi_df['Ask Size '+str(i)][ask_equal]
        OFI[ask_less] = -ofi_df['Ask Size '+str(i)][ask_less]
        ofi_df['OFI'] = OFI
        ofi_df_period = ofi_df[ofi_df.index.isin(df_period_level.index)]
        ofi[i-1] = ofi_df_period.OFI.sum()
    return ofi
'''


def get_cfi_level(df_period_cancels,normalise_cfi = False):
    cfi = np.zeros(shape = (10,))
    
    for level in range(1,10+1):
        df_period_cancels_level = df_period_cancels[ df_period_cancels.Level == level ]
        if df_period_cancels_level.shape[0] == 0:
            continue
        else:
            bid_volume = df_period_cancels_level[(df_period_cancels_level.TradeDirection == 1)].Size.sum()
            ask_volume = df_period_cancels_level[(df_period_cancels_level.TradeDirection == -1)].Size.sum()
            cfi[level-1] = (bid_volume-ask_volume)
            if normalise_cfi:
                cfi[level-1] = cfi[level-1]/(bid_volume+ask_volume)
    
    return cfi


def get_ofi_level(df_period_msg,df_ob,normalise_ofi = False):
    ofi = np.zeros(shape = (10,))
    total_depth = np.zeros(shape = (10,))
    n_events = df_period_msg.shape[0]
    
    for level in range(1,10+1):
        df_period_level = df_period_msg[(df_period_msg.Level == level)]
        df_period_level_bid =  df_period_level[ (df_period_level.TradeDirection == 1)]
        df_period_level_ask = df_period_level[(df_period_level.TradeDirection == -1)]
        
        flow_bid = df_period_level_bid[df_period_level_bid.Type == 1].Size.sum() - df_period_level_bid[df_period_level_bid.Type == 2].Size.sum() - df_period_level_bid[df_period_level_bid.Type == 3].Size.sum() -  df_period_level_bid[df_period_level_bid.Type == 4].Size.sum()
        flow_ask = df_period_level_ask[df_period_level_ask.Type == 1].Size.sum() - df_period_level_ask[df_period_level_ask.Type == 2].Size.sum() - df_period_level_ask[df_period_level_ask.Type == 3].Size.sum() - df_period_level_ask[df_period_level_ask.Type == 4].Size.sum() 

        ofi[level-1] = flow_bid - flow_ask
        
    for level in range(1,11):
        total_depth[level-1] = df_ob[ df_ob.index.isin(df_period_msg.index) ]['Ask Size '+str(level)].sum() + df_ob[ df_ob.index.isin(df_period_msg.index) ]['Bid Size '+str(level)].sum() 
    
    normaliser = np.sum( total_depth )/(2*n_events)
    if normalise_ofi and normaliser!=0 and n_events!=0:
        return ofi/normaliser
    else:
        return ofi
        

def get_returns(df_period_msg_,which_return = 1):
    df_period_msg = df_period_msg_.copy()
    
    if (df_period_msg.shape[0]) == 0:
        # no entries case
        mid_price_change = 0 
        returns = 0 
        log_returns = 0 
    else:
        best_end = df_period_msg.iloc[df_period_msg.shape[0]-1]
        if (best_end.BestBidSize == 0) & (best_end.BestAskSize != 0):
            mid_price_end = best_end.BestAsk
        elif (best_end.BestBidSize != 0) & (best_end.BestAskSize == 0):
            mid_price_end = best_end.BestBid
        elif (best_end.BestBidSize == 0) & (best_end.BestAskSize == 0):
            mid_price_end = 0
        else:
            mid_price_end = 0.5*(best_end.BestBid + best_end.BestAsk)
            
        best_start = df_period_msg.iloc[0]
        if (best_start.BestBidSize == 0) & (best_start.BestAskSize != 0):
            mid_price_start = best_start.BestAsk
        elif (best_start.BestBidSize != 0) & (best_start.BestAskSize == 0):
            mid_price_start = best_start.BestBid
        elif (best_start.BestBidSize == 0) & (best_start.BestAskSize == 0):
            mid_price_start = 0
        else:
            mid_price_start = 0.5*(best_start.BestBid + best_start.BestAsk)
        
        mid_price_change = mid_price_end - mid_price_start
        if mid_price_start == 0:
            log_returns = 0
            returns = 0
        else:
            returns = (mid_price_end - mid_price_start)/mid_price_start
            log_returns = np.log(mid_price_end/mid_price_start)
    if which_return == 1:
        return log_returns
    elif which_return == 2:
        return returns
    else:
        return mid_price_change


def extract_features_day(df_msg_expanded,df_cancels_expanded,df_ob,freq_sample_mins = 1, normalise_ofi = False, normalise_cfi = False):
    start_time = 10*60*60
    end_time = 15.5*60*60
    freq = int(freq_sample_mins*60)
    
    ofi = np.zeros( shape = (int((end_time - start_time)/freq),10) )
    cfi = np.zeros( shape = (int((end_time - start_time)/freq),10) )
    
    log_returns = np.zeros( shape = (int((end_time - start_time)/freq),) )
    arr_index = 0
    
    for i in range(int(start_time) + freq ,int(end_time) + freq,freq):
        period_start = i - freq
        period_end = i
        df_period_msg = df_msg_expanded[ (df_msg_expanded.Time > period_start) & (df_msg_expanded.Time <= period_end) ]
        df_period_cancels = df_cancels_expanded[ (df_cancels_expanded.Time > period_start) & (df_cancels_expanded.Time <= period_end) ]
        
        ofi[arr_index] = get_ofi_level(df_period_msg,df_ob,normalise_ofi)
        cfi[arr_index] = get_cfi_level(df_period_cancels,normalise_cfi)
        log_returns[arr_index] = get_returns(df_period_msg)

        arr_index+=1
        
    return cfi,ofi,log_returns 