# -*- coding: utf-8 -*-
"""
@author: pancho
"""
import pandas as pd
import numpy as np
import libarchive


def get_level_levelSize(df_msg,ob_dict,Direction = 1):
    '''
    match price level to order event for a specific direction (bid(1)/ask(-1))
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
    
    
def summary_ids(df_msg, minutes = [1,5,10,30,60,120,300]):
    '''
    Deprecated: Used only for intial analysis of data
    create summaries for processed orders on each day 
    '''
    received_order_ids = df_msg[df_msg.Type == 1].OrderID
    prev_days_order_ids = df_msg[~df_msg.OrderID.isin(received_order_ids)].OrderID.unique()
    
    df_msg_cancelled_arrive_today = df_msg[(df_msg.OrderID.isin(received_order_ids)) & (df_msg.Type == 3)]
    df_msg_cancelled_arrive_prev = df_msg[(df_msg.OrderID.isin(prev_days_order_ids)) & (df_msg.Type == 3)]
      
    # orders that have arrived and got completely cancelled within the same day
     # minutes within which orders are cancelled
    dict_dif = dict.fromkeys(minutes,0)
    dict_dif['more'] = 0
    dict_cancelled_levels = {key: dict.fromkeys([i for i in range(1,11)],0) for key in dict_dif.keys()}
    
    # fully cancelled
    cancelled_ids_len = 0
    
    # not cancelled (maybe partially), executed
    executed_ids_len = 0
    
    # orders that have arrived and remain in order book at the end of the day
    # not cancelled (maybe partially), (partially) executed that remain in order book 
    remain_ids_len = 0

    for i in range(len(received_order_ids)):
        
        id_ = received_order_ids.iloc[i]
        df_id = df_msg[df_msg.OrderID == id_]
        
        submissions = df_id[df_id.Type == 1]
        partial_cancels = df_id[df_id.Type == 2]
        cancels = df_id[df_id.Type == 3]
        executions = df_id[df_id.Type == 4]
        
        if (cancels.shape[0] == 0):
            order_size = submissions.Size.iloc[0]
            partial_cancel = partial_cancels.Size.sum()
            partial_execution = executions.Size.sum()
            remain = order_size - partial_cancel - partial_execution
            if remain > 0:
                # not cancelled (maybe partially), (partially) executed that remain in ob
                remain_ids_len+=1
            else: 
                # not cancelled (maybe partially), executed
                executed_ids_len+=1

        # cancelled        
        elif(cancels.shape[0] == 1):
            cancelled_ids_len+=1
            arrive = submissions.Time.iloc[0]
            cancel = cancels.Time.iloc[0]
            dif_min = (cancel - arrive)/60
            
            if dif_min > np.max(minutes):
                dict_cancelled_levels['more'][cancels.Level.iloc[0]]+=1
            else:
                dict_cancelled_levels[minutes[np.argmax(dif_min<=minutes)]][cancels.Level.iloc[0]]+=1

    
    summary_dict = {'ProcessedNew':len(received_order_ids),
        'RemainingNew':remain_ids_len,
        'ExecutedNew':executed_ids_len,
        'CancelledNew':cancelled_ids_len,
        'ProcessedPrev': len(prev_days_order_ids),
        'CancelledPrev':df_msg_cancelled_arrive_prev.shape[0],
        'CancelledNewMeanPortionLvl': (df_msg_cancelled_arrive_today.Size/df_msg_cancelled_arrive_today.LevelSize).mean(),
        'CancelledPrevMeanPortionLvl': (df_msg_cancelled_arrive_prev.Size/df_msg_cancelled_arrive_prev.LevelSize).mean()  }
    
    arrive_prev_cancel_breakdown = pd.DataFrame(df_msg_cancelled_arrive_prev.Level.value_counts().sort_index())
    arrive_prev_cancel_breakdown.columns = ['NoOrders']
    
    return pd.DataFrame([summary_dict]),pd.DataFrame(dict_cancelled_levels),arrive_prev_cancel_breakdown
    

def load_message_contents(entry,trading_hours = True):
    colnames = ["Time" , "Type" , "OrderID" , "Size" , "Price" , "TradeDirection","Message"]
    # load text of file
    content_bytes = bytearray("", encoding="utf_8")
    for block in entry.get_blocks():
        content_bytes += block
    content = content_bytes.decode(encoding="utf_8")[:-1]
    jsonObjs = content.split("\n")
    
    return pd.DataFrame([x.split(',') for x in jsonObjs],columns = colnames,dtype='float64') 

def load_ob_contents(entry,nlevels = 10):
    names = ['Ask Price ','Ask Size ','Bid Price ','Bid Size ']
 
    colnames = []
    for i in range(1, nlevels + 1):
        for j in names:
            colnames.append(str(j) + str(i))
        content_bytes = bytearray("", encoding="utf_8")
    
    for block in entry.get_blocks():
        content_bytes += block
    content = content_bytes.decode(encoding="utf_8")[:-1]
    jsonObjs = content.split("\n")

    ob = pd.DataFrame([x.split(',') for x in jsonObjs],columns = colnames,dtype='float64')  
    # ob.index = index

    ob_ask_price = ob[ob.columns[range(0,len(ob.columns),4)]]
    ob_ask_size = ob[ob.columns[range(1,len(ob.columns),4)]]
    ob_bid_price = ob[ob.columns[range(2,len(ob.columns),4)]]
    ob_bid_size = ob[ob.columns[range(3,len(ob.columns),4)]]
    
    ob_dict = {'ob_ask_price':ob_ask_price,'ob_ask_size':ob_ask_size,'ob_bid_price':ob_bid_price,'ob_bid_size':ob_bid_size}
    
    return ob,ob_dict


def search_matching_ob(zip_file_name,msg_entry):
    with libarchive.file_reader(zip_file_name) as ee:
            for entry2 in ee:
                if str(entry2) == msg_entry:
                    ob,ob_dict = load_ob_contents(entry2)
                    return ob,ob_dict
                

def count_trading_days(zip_file_name):
    count = 0
    with libarchive.file_reader(zip_file_name) as e:
        for entry in e:

            # name of each file in zipped folder
            individual_file_name = str(entry)

            # only consider desired file type message/orderbook
            if ('message' not in individual_file_name):
                continue
            count+=1
    return count

def get_mid_price(df_msg,ob_dict):
    df_msg['BestAsk'] = ob_dict['ob_ask_price']['Ask Price 1']/10000
    df_msg['BestBid'] = ob_dict['ob_bid_price']['Bid Price 1']/10000
    df_msg['MidPrice'] = (df_msg['BestBid'] + df_msg['BestAsk'] )/2
    
def expand_df_msg(df_msg,ob_dict):
    get_mid_price(df_msg,ob_dict)
    get_level_levelSize_all(df_msg,ob_dict)

def cancels_expanded(df_msg_expanded):
    new_df = df_msg_expanded[(df_msg_expanded.Type == 3)].merge(df_msg_expanded[(df_msg_expanded.Type == 1)][['OrderID','Size','Time','LevelSize']],how = 'left',on = 'OrderID',suffixes=('', 'Arrival'))
    new_df.index = df_msg_expanded[(df_msg_expanded.Type == 3)].index
    new_df['AgeMins'] = (new_df['Time'] - new_df['TimeArrival'])/60
    return new_df

def partial_cancels_expanded(df_msg_expanded):
    new_df = df_msg_expanded[(df_msg_expanded.Type == 2)].merge(df_msg_expanded[(df_msg_expanded.Type == 1)][['OrderID','Size','Time','LevelSize']],how = 'left',on = 'OrderID',suffixes=('', 'Arrival'))
    new_df.index = df_msg_expanded[(df_msg_expanded.Type == 2)].index
    new_df['AgeMins'] = (new_df['Time'] - new_df['TimeArrival'])/60
    return new_df

def expand_df_cancels(df_msg):
    df_cancels_expanded = cancels_expanded(df_msg)
    df_partial_cancels_expanded = partial_cancels_expanded(df_msg)
    df_cancels = pd.concat((df_partial_cancels_expanded,df_cancels_expanded))
    df_cancels.sort_index(inplace = True)
    return df_cancels



def load_msg_ob(zip_file_name,entry,extended_msg = True):
    df_msg = load_message_contents(entry)
    ob_file_name = str(entry).replace('message','orderbook')
    df_ob,ob_dict = search_matching_ob(zip_file_name,ob_file_name)
    if extended_msg:
        expand_df_msg(df_msg,ob_dict)
    return df_msg,df_ob,ob_dict

'''
for Chao's implementation 


# def extract_cfi_period(df,period_start,period_end,freq_period):
#     cfi = np.zeros( shape = (int((period_end - period_start)/freq_period)) )
#     returns = np.zeros( shape = (int((period_end - period_start)/freq_period)) )
#     arr_index = 0
#     for j in range(period_start,period_end,freq_period):
#         df_period = df[ (df.Time > j) & (df.Time <= j+freq_period) & (df.Level == 1) ]
#         if df_period.shape[0] == 0:
#             cfi[arr_index] = 0
#             returns[arr_index] = 0
#         else:
#             bid_volume = df_period[(df_period.TradeDirection == 1)].Size.sum()
#             ask_volume = df_period[(df_period.TradeDirection == -1)].Size.sum()
#             cfi[arr_index] = (bid_volume-ask_volume)/(bid_volume + ask_volume)
#             returns[arr_index] = df_period.MidPrice.iloc[df_period.MidPrice.shape[0]-1] - df_period.MidPrice.iloc[0]
#         arr_index+=1
#     return returns,cfi

# def extract_cfi(df_cancels_expanded,freq_sample = 30,freq_period = 10):
#     start_time = 10*60*60
#     end_time = 15.5*60*60
#     freq = freq_sample*60
#     n_observations_sample = int(freq_sample*60/freq_period)
    
#     cfi_day = np.zeros( shape = (int((end_time - start_time)/freq),n_observations_sample) )
#     returns_day = np.zeros( shape = (int((end_time - start_time)/freq),n_observations_sample) )
#     arr_index = 0
    
#     for i in range(int(start_time) + freq ,int(end_time),freq):
#         period_start = i - freq
#         period_end = i
#         df_period_sample = df_cancels_expanded[ (df_cancels_expanded.Time > period_start) & (df_cancels_expanded.Time <= period_end) ]
#         cfi_day[arr_index,:],returns_day[arr_index,:] =  extract_cfi_period(df_period_sample,period_start,period_end,freq_period)
#         arr_index+=1
#     return cfi_day,returns_day


'''
