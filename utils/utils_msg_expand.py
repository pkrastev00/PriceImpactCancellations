# -*- coding: utf-8 -*-
"""
@author: pancho
"""

import pandas as pd
import numpy as np

# added features to message df: 
'''
# Level: Price Level position at event trigger (after arrival for 
sumbissions/before (partial)cancellation/execution)

# LevelSize: Level size at event trigger (after arrival for 
sumbissions/before (partial)cancellation/execution)

# BestBid: Best Bid

# BestAsk: Best Ask

# MidPrice: Mid Price
'''

def get_level_levelSize(df_msg,ob_dict,Direction = 1):
    '''
    match price level and level size to order event for a specific direction (bid(1)/ask(-1))
    for partial cancels/cancels/executions matching is done for previous state of the book 
    '''
    if Direction == 1:
        price_type = 'ob_bid_price'
        size_type = 'ob_bid_size'
    if Direction == -1:
        price_type = 'ob_ask_price'
        size_type = 'ob_ask_size'
    n_levels = 10
    
    # update only order arrive level
    msg = df_msg[ (df_msg.TradeDirection == Direction) & (df_msg.Type == 1) ]
    msg_price = msg.Price
    prices = ob_dict[price_type][ (df_msg.TradeDirection == Direction) & (df_msg.Type == 1) ]
    prices.columns = [i for i in range(1,n_levels+1)]
    level_match = prices.eq(msg_price,axis = 0)
    level = level_match.idxmax( axis = 1)
    ob_dict[size_type].columns = [i for i in range(1,n_levels+1)]
    level_size = ob_dict[size_type].lookup(level.index,level.values)
    level_size = pd.Series(level_size,index=level.index)

    # update order cancels, partial cancel, execution: 
    # use state of order book before event, so that we know where order was residing
    ind_original = (df_msg.TradeDirection == Direction) & (df_msg.Type.isin([2,3,4]))
    ind_back = np.roll(ind_original,-1)
    ind_back[(len(ind_back)-1)] = False
    msg_ = df_msg[ ind_original ]
    if msg_.index[0] == 0:
        msg_.drop(labels = 0, axis = 'index',inplace=True)
    msg_price_ = msg_.Price
    msg_price_.index = np.where(ind_back)[0]

    level_prices_ = ob_dict[price_type][ ind_back ]
    level_prices_.columns = [i for i in range(1,n_levels+1)]
    level_match_ = level_prices_.eq(msg_price_,axis = 0)
    level_ = level_match_.idxmax( axis = 1)

    level_size_ = ob_dict[size_type][ ind_back ]
    level_size_.columns = [i for i in range(1,n_levels+1)]
    level_size_match_ = level_size_.lookup(level_.index,level_.values)
    level_size_match_ = pd.Series(level_size_match_,index=level_.index)
    
    level_.index+=1
    level_size_match_.index += 1

    # fill out order type 5 information
    level_5 = pd.Series(-1,index = df_msg[ (df_msg.TradeDirection == Direction) & (~df_msg.Type.isin([1,2,3,4])) ].index)
    level_size_5 = pd.Series(-1,index = df_msg[ (df_msg.TradeDirection == Direction) & (~df_msg.Type.isin([1,2,3,4])) ].index)
    
    # combine results
    level_final = pd.concat([level,level_,level_5]).sort_index()
    size_final = pd.concat([level_size,level_size_match_,level_size_5]).sort_index()
    return level_final,size_final


def get_level_levelSize_all(df_msg,ob_dict):
    '''
    match price level to order event for a both directions (bid(1)/ask(-1))
    '''
    level_final_buy,size_final_buy =  get_level_levelSize(df_msg,ob_dict,Direction = 1)
    level_final_sell,size_final_sell =  get_level_levelSize(df_msg,ob_dict,Direction = -1)
    level = pd.concat([level_final_buy,level_final_sell]).sort_index()
    size = pd.concat([size_final_buy,size_final_sell]).sort_index()
    df_msg['Level'] = level
    df_msg['LevelSize'] = size
    

def get_mid_price(df_msg,ob_dict):
    df_msg['BestAsk'] = ob_dict['ob_ask_price']['Ask Price 1']/10000
    df_msg['BestBid'] = ob_dict['ob_bid_price']['Bid Price 1']/10000
    df_msg['BestAskSize'] = ob_dict['ob_ask_size']['Ask Size 1']
    df_msg['BestBidSize'] = ob_dict['ob_bid_size']['Bid Size 1']
    df_msg['MidPrice'] = (df_msg['BestBid'] + df_msg['BestAsk'] )/2
    df_msg['MidPriceWeighted'] = (df_msg['BestBidSize']*df_msg['BestBid'] + df_msg['BestAskSize']*df_msg['BestAsk'] )/( df_msg['BestBidSize'] + df_msg['BestAskSize'])
    
def expand_df_msg(df_msg,ob_dict):
    get_mid_price(df_msg,ob_dict)
    get_level_levelSize_all(df_msg,ob_dict)
    #df_msg.drop(df_msg[ (df_msg.BestBidSize == 0)|(df_msg.BestAskSize == 0) ].index,inplace = True) # remove those entries where one side of book is empty
    #df_msg['Spread'] = df_msg['BestAsk'] - df_msg['BestBid']
    #df_msg['AbsDistanceToMid'] = (df_msg['Price'] - df_msg['MidPrice']).abs()