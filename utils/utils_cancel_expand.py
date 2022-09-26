# -*- coding: utf-8 -*-
"""
@author: pancho
"""
import pandas as pd

# added features to cancellation df: 
'''
# SizeArrival: Order size at arrival- can tell us if order has been 
partially cancelled/executed before cancel

# TimeArrival: Arrival time of order

# LevelArrival: Price level at which order has arrived- can tell us if orders 
at better price levels have been submitted and that is why it is getting 
canceled

# LevelSizeArrival: Size of level at which order has arrived- can tell us if
order was submited at a better price level than all existing orders, i.e. if 
order was more aggresive

# AgeMins: Age of order

# Queue: Queue position of order at cancel, relative to orders that have arrived
on the specific day with price within the top 10 levels at arrival
'''

def get_cancel_arrival_information(df_msg,keep_UNKNOWN = False):
    df_msg_arrived_today = df_msg[df_msg.Type == 1]
    orders_arrived_today_10lvl = df_msg_arrived_today.OrderID
    if keep_UNKNOWN:
        df_cancel = df_msg[( (df_msg.Type == 2) | (df_msg.Type == 3) )]
    else:
        df_cancel = df_msg[ df_msg.OrderID.isin(orders_arrived_today_10lvl) & ( (df_msg.Type == 2) | (df_msg.Type == 3) )]
        
    new_df = df_cancel.merge(df_msg_arrived_today[ ['OrderID','Size','Time','Level','LevelSize'] ],how = 'left',on = 'OrderID',suffixes=('', 'Arrival'))
    new_df.index = df_cancel.index
    new_df['AgeMins'] = (new_df['Time'] - new_df['TimeArrival'])/60
    return new_df


def get_queue_direction(df_msg,Direction):
    orders_arrived_today_10lvl = df_msg[df_msg.Type == 1].OrderID
    # remove orders arrived on previous days or outside of top 10 levels
    df_msg_keep = df_msg[df_msg.OrderID.isin(orders_arrived_today_10lvl)] 
    # keep only bid/ask side
    df_msg_keep_direction = df_msg_keep[df_msg_keep.TradeDirection == Direction]
    # keep only cancels and partial cancels
    df_msg_keep_direction_cancel = df_msg_keep_direction[(df_msg_keep_direction.Type == 2) | (df_msg_keep_direction.Type == 3)]
    df_msg_keep_direction_cancel_ = df_msg_keep_direction_cancel[['OrderID','Time','Type','Price','Size']]

    # match each (partially) cancelled order with all other orders at same price level
    a = df_msg_keep_direction_cancel_.merge(df_msg_keep_direction[['OrderID','Size','Time','Price','Type']],how = 'left',on = 'Price',suffixes=('', '_OrderB'))
    
    # keep orders at price level only if they have arrived before the (partially) cancelled order
    b = a.loc[a.Time_OrderB <= a.Time]
    # change size to negative for (partial) cancels and executions
    b1 = b.copy()
    b1.loc[b1.Type_OrderB != 1,'Size_OrderB'] = -b.loc[b.Type_OrderB != 1,'Size_OrderB']
    
    aggregation_functions = {'Size_OrderB':'sum','Time_OrderB':'first'}
    c = b1.groupby(['OrderID','Type','Time','OrderID_OrderB'],as_index=False).agg(aggregation_functions)
    
    # remove orders that have been executed/canceled (aggregate volume 0) at the price level of each (partial) cancel order
    d = c.loc[ ((c.OrderID != c.OrderID_OrderB) & (c.Size_OrderB > 0)) | (c.OrderID == c.OrderID_OrderB)]
    # reconstruct queue from remaining orders
    d1 = d.copy()
    d1['Queue'] = d.groupby(['OrderID','Type','Time'],as_index=False)['Time_OrderB'].rank("dense", ascending=True).Time_OrderB

    # keep only entries that correspond to the original (partial) cancellation events
    e = d1[d1.OrderID == d1.OrderID_OrderB]

    # connect queue position to original cancellation orders in order to keep original ordering
    f = df_msg_keep_direction_cancel.merge(e[['OrderID','Type','Time','Queue']],how = 'left',on = ['OrderID','Type','Time'],suffixes=('', ''))
    f.index = df_msg_keep_direction_cancel.index
    
    return f

def get_queue(df_msg):
    df_cancel_queue_bid = get_queue_direction(df_msg,1)
    df_cancel_queue_ask = get_queue_direction(df_msg,-1)
    df_cancel_queue = pd.concat([df_cancel_queue_bid,df_cancel_queue_ask]).sort_index()
    return df_cancel_queue

def expand_df_cancel(df_msg, QUEUE = False):
    #if simple:
    #   orders_arrived_today_10lvl = df_msg[df_msg.Type == 1].OrderID
    #  return df_msg[ df_msg.OrderID.isin(orders_arrived_today_10lvl) & ( (df_msg.Type == 2) | (df_msg.Type == 3) )]
    df_cancel_arrival = get_cancel_arrival_information(df_msg)
    if QUEUE:
        df_cancel_queue = get_queue(df_msg)
        df_cancel_arrival['Queue'] =  df_cancel_queue['Queue']
    return df_cancel_arrival
