# -*- coding: utf-8 -*-
"""
@author: pancho
"""

import pandas as pd
import libarchive


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

def load_message_contents(entry):
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

def search_matching_ob(zip_file_name,ob_file_name):
    with libarchive.file_reader(zip_file_name) as ee:
            for entry2 in ee:
                if str(entry2) == ob_file_name:
                    ob,ob_dict = load_ob_contents(entry2)
                    return ob,ob_dict
                
def load_msg_ob(zip_file_name,entry):
    df_msg = load_message_contents(entry)
    ob_file_name = str(entry).replace('message','orderbook')
    df_ob,ob_dict = search_matching_ob(zip_file_name,ob_file_name)
    return df_msg,df_ob,ob_dict
