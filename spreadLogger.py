from binance.spot import Spot
import logging
import pandas as pd
import threading
import traceback
import time

#spreadLogger class code

class spreadLogger(threading.Thread):
    
    def __init__(self,numSyms,quoteAsset,key='volume',lim=100,base_URL='https://api.binance.com'):
        super().__init__()
        self.numSyms = numSyms
        self.quoteAsset = quoteAsset
        self.key = key
        self._stopper = threading.Event()
        self.spreadData = pd.DataFrame({'symbol':['DUMMY'],'spreads':[[0.0 for i in range(lim)]]}).set_index('symbol')
        self.client = Spot(base_URL)
        self.limit = lim
        self._isConnected = False
        logging.basicConfig(filename='loggings.log', level=logging.INFO, format ="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s")
    
    def isConnected(self):
        try:
            if self.client.ping() == dict():
                self._isConnected = True
        except Exception as e:
            logging.error("Unable to connect to {}  due to error : {}".format(self.client.base_url,e))
            self._isConnected = False
        return self._isConnected

    def stopit(self):
        self._stopper.set()
        
    def isStopped(self):
        return self._stopper.is_set()
    
    def getNotionalValues(self,symbols,top):
        res = {'symbol':[], 'totalNotionalBids':[], 'totalNotionalAsks':[]}
        #get orders
        orders = self.getOrderData(symbols)

        #for i in range(len(symbols)):
        for order in orders:
            res['symbol'].append(order['symbol'])

            #if data was retrievable from database
            #if orders[i]['lastUpdateId'] != -1: 
            if order['payload']['lastUpdateId'] != -1:
                bids = sorted(order['payload']['bids'], key=lambda x:x[0], reverse=True)
                asks = sorted(order['payload']['asks'], key=lambda x:x[0], reverse=True)

                if len(bids) > top:
                    bids = bids[:top]
                if len(asks) > top:
                    asks = asks[:top]

                ttlBids = sum([float(tup[0])*float(tup[1]) for tup in bids])
                ttlAsks = sum([float(tup[0])*float(tup[1]) for tup in asks])

                #res[str(sym)] = {'totalNotionalBids':ttlBids, 'totalNotionalAsks':ttlAsks}
                res['totalNotionalBids'].append(ttlBids)
                res['totalNotionalAsks'].append(ttlAsks)
            else:
                res['totalNotionalBids'].append(0.0)
                res['totalNotionalAsks'].append(0.0)
                #res[str(sym)] = {'totalNotionalBids':0.0, 'totalNotionalAsks':0.0}
        df = pd.DataFrame(data=res)
        return df.set_index('symbol')
    
    def topQuoteAsset(self,num=None,quoteAsset=None,key=None,descending=True):
        num = num or self.numSyms
        quoteAsset = quoteAsset or self.quoteAsset
        key = key or self.key
        
        #should have a case for server not available
        if not self.isConnected():
            logging.warn("server is not connected. Aborting function")
            return None

        #get symbols from exchange_info
        try:
            data = self.client.exchange_info()
            symInfo = data['symbols']
            syms = [sym['symbol'] for sym in symInfo if sym['quoteAsset'] == quoteAsset]

            if not len(syms):
                return []

            #get 24 hour Market data from filtered symbols
            data = self.client.ticker_24hr(symbols=syms)
            res = sorted([(d['symbol'],float(d[key])) for d in data], key=lambda x:x[1], reverse=descending)

            if len(res) > num:
                res = res[:num]
            logging.info("topQuoteAsset results are : %s" % res)
            return [r[0] for r in res]
        
        except Exception as e:
            logging.error("unable to perform request due to error : {}".format(e))
        return None

    def getOrderData(self,symbols):
        orders = []
        for sym in symbols:
            try:
                orders.append({'symbol':sym,'payload':self.client.depth(sym,limit=self.limit)})
            except Exception as e:
                logging.error("unable to perform request due to error : {}".format(e))
                orders.append({'symbol':sym,'payload':{'lastUpdateId':-1.0, 'bids':[],'asks':[]}})
        return orders
    
    def getSpreads(self,symbols):
        res = {'symbol':[], 'spreads':[]}

        orders = self.getOrderData(symbols)

        for order in orders:
            res['symbol'].append(order['symbol'])

            #calculate the order spreads
            if order['payload']['lastUpdateId'] != -1.0:
                bids = [bid[0] for bid in order['payload']['bids']]
                asks = [ask[0] for ask in order['payload']['asks']]

                spreads = []
                for m,n in zip(bids,asks):
                    spreads.append(abs(float(m)-float(n)))
                res['spreads'].append(spreads)
            else:
                res['spreads'].append([0.0])
        df = pd.DataFrame(data=res)
        return df.set_index('symbol')
    
    def produceDeltas(self,sym,latest):
        
        curr = latest.loc[sym]
        prev = self.spreadData.loc[self.spreadData.index == sym]
        
        #if sym exists, print absolute delta and replace entry for sym
        if len(prev):
            prev = prev.loc[sym]
                
            abs_deltas = [abs(i-j) for i,j in zip(curr['spreads'],prev['spreads'])]
            self.spreadData.loc[sym]['spreads'] = curr['spreads']
            
            return abs_deltas
        #else, print current price and concat
        else:
            tmp = pd.DataFrame({'symbol':[sym],'spreads':[curr['spreads']]}).set_index('symbol')
            self.spreadData = pd.concat([self.spreadData,tmp])
            
            return curr['spreads']
            
            
    def printDeltas(self,latest):
        #if symbols in latest doesn't exist then return 0b
        if 'symbol' != latest.index.name:
            logging.error("latest must have index name 'symbol'")
            return False
        
        for sym in latest.index.tolist():
            
            deltas = self.produceDeltas(sym,latest)
            
            logging.info("deltas for sym %s are %s " % (sym,deltas))
            
        return True
    
        
    def run(self):        
        while not self._stopper.is_set():
            try:

                top_assets = self.topQuoteAsset()
                
                if not top_assets:
                    logging.info("no symbols retrieved, aborting thread")
                    time.sleep(10)
                    continue
                    
                new = self.getSpreads(top_assets)
                self.printDeltas(new)
                
                time.sleep(10)
            except:
                logging.error("execption caught while running thread")
                traceback.print_exc()
                self.stopit()
