from spreadLogger import *
from spreadCollector import *
import pytest
from unittest import mock

class TestSpreadLogger:
    
    slo = spreadLogger(100,'BTC',base_URL="https://testnet.binance.vision")

    pdData = pd.DataFrame({ 
        'symbol':['BNBUSDT','BTCUSDT','ETHUSDT'],
        'spreads':[
            [0,0,2,1,0],
            [9,0,2,3,0],
            [7,6,4,1,3]
        ]
    }).set_index('symbol')
        
    mock_data_1 = [{'symbol': 'BNBBTC',
                              'openPrice': '0.01',
                              'highPrice': '0.04',
                              'lowPrice': '0.005',
                              'volume': '22',
                              'count': 900},
                             {'symbol': 'ETHBTC',
                              'openPrice': '0.02',
                              'highPrice': '0.06',
                              'lowPrice': '0.05',
                              'volume': '19',
                              'count': 1200},
                             {'symbol': 'LTCBTC',
                              'openPrice': '0.0021',
                              'highPrice': '0.0025',
                              'lowPrice': '0.0019',
                              'volume': '60',
                              'count': 450},
                             {'symbol': 'TRXBTC',
                              'openPrice': '0.000001',
                              'highPrice': '0.00001',
                              'lowPrice': '0.0000006',
                              'volumn':'10',
                              'count': 260},
                             {'symbol': 'XRPBTC',
                              'openPrice': '0.0008',
                              'highPrice': '0.0009',
                              'lowPrice': '0.00006',
                              'volume': '99',
                              'count': 712}]
 
    mock_data_2 = [{'symbol': 'ETHBTC',
                    'payload': {
                        'lastUpdateId': 11102.0, 
                        'bids':[[9.72, 4], [1.37, 3], [5.93, 2], [8.12, 4], [8.12, 2], [3.94, 5], [8.01, 3], [8.4, 1], [4.9, 4], [5.95, 2], [6.2, 2], [4.27, 5]],
                        'asks':[[4.65, 2], [1.43, 5], [1.94, 5], [4.4, 3], [2.55, 1], [3.94, 2], [6.53, 5], [3.39, 5], [8.78, 4], [4.4, 2], [3.52, 3], [2.46, 3]]
                    }},
                   {'symbol': 'BNBBTC',
                    'payload':{
                        'lastUpdateId': 11103.0,
                        'bids':[[1.36, 4], [8.61, 3], [1.13, 2], [2.65, 4], [6.69, 1], [3.55, 4], [9.06, 2], [6.35, 3], [9.06, 2], [5.72, 2], [8.49, 3], [5.79, 2]],
                        'asks':[[9.75, 5], [1.14, 1], [1.47, 1], [7.51, 2], [4.87, 3], [7.01, 4], [4.87, 4], [1.6, 5], [4.6, 2], [7.72, 2], [4.69, 4], [2.0, 1]]
                    }},
                   {'symbol':'XRPBTC',
                    'payload':{
                        'lastUpdateId': -1.0,
                        'bids':[],
                        'asks':[]
                    }}
                  ]

    mock_data_3 = [{'symbol':'ETHUSDT',
                    'payload': {
                        'lastUpdateId': 11105.0,
                        'bids' : [[2,2],[7,7],[1,1],[10,10],[3,3]],
                        'asks' : [[3, 3], [9, 9], [4, 4], [2, 2], [4, 4]]
                    }},
                    {'symbol':'BTCUSDT',
                    'payload': {
                        'lastUpdateId': 11106.0,
                        'bids' : [[8, 8], [10, 10], [6, 6], [2, 2], [6, 6]],
                        'asks' : [[9, 9], [9, 9], [4, 4], [7, 7], [6, 6]]
                    }},
                   {'symbol':'BNBUSDT',
                    'payload': {
                        'lastUpdateId': 11109.0,
                        'bids' : [[7, 7], [6, 6], [6, 6], [2, 2], [5, 5]],
                        'asks' : [[4, 4], [9, 9], [1, 1], [5, 5], [3, 3]]
                    }},
                   {'symbol':'LTCUSDT',
                    'payload': {
                        'lastUpdateId': 11112.0,
                        'bids' : [[8, 8], [0, 0], [1, 1], [0, 0], [4, 4]],
                        'asks' : [[6, 6], [9, 9], [8, 8], [2, 2], [1, 1]]
                    }},
                   {'symbol':'TRXUSDT',
                    'payload': {
                        'lastUpdateId': 11113.0,
                        'bids' : [[2, 2], [2, 2], [4, 4], [6, 6], [9, 9]],
                        'asks' : [[3, 3], [9, 9], [6, 6], [1, 1], [3, 3]]
                    }},
                   ]

   
    def test_invalid_endPoint(self):
        self.slo.client = Spot(base_url="https://non-valid-endpoint")
        
        assert False == self.slo.isConnected()
        
        assert not self.slo.topQuoteAsset()
        
        self.slo.client = Spot(base_url="https://testnet.binance.vision")
        return
    
    #@mock.patch('binance.spot.Spot.depth')
    def test_topQuoteAsset_rightAssets(self):
        data = self.slo.topQuoteAsset(quoteAsset='BUSD',key='volume')
        
        assert all(['BUSD' in d for d in data])
        
        data = self.slo.topQuoteAsset(quoteAsset='BNB',key='askQty')
        
        assert all(['BNB' in d for d in data])
   
    @mock.patch('binance.spot.Spot.ticker_24hr',
                return_value=mock_data_1)
    def test_topQuoteAsset_rightKey(self,mock_args):
        
        data = self.slo.topQuoteAsset(quoteAsset='BTC',key='count')
        
        assert ['ETHBTC','BNBBTC','XRPBTC','LTCBTC','TRXBTC'] == data

        data = self.slo.topQuoteAsset(quoteAsset='BTC',key='highPrice')
        
        assert ['ETHBTC','BNBBTC','LTCBTC','XRPBTC','TRXBTC'] == data

        return

    def test_topQuoteAsset_numRet(self):
        
        data = self.slo.topQuoteAsset(num=100000)
        
        assert len(data) < self.slo.numSyms
        
    def test_topQuoteAsset_keyNoExist(self):
        
        data = self.slo.topQuoteAsset(key='superPrice')
        
        assert not data
        
    def test_topQuoteAsset_noQuoteAsset(self):
        
        data = self.slo.topQuoteAsset(quoteAsset='ETH')
        
        assert len(data) == 0

    def test_getOrderData(self):
        
        data = self.slo.getOrderData(['BNBBTC','ETHBTC','ETHUSDT','USDJPY'])
        
        faulty = data[3]
        
        assert faulty['symbol'] == 'USDJPY'
        
        assert faulty['payload']['lastUpdateId'] == -1.0
        
        assert faulty['payload']['bids'] == [] and faulty['payload']['asks'] == []

    @mock.patch('spreadLogger.spreadLogger.getOrderData',return_value=mock_data_2)
    def test_getNotionalValues(self,mock_args):
        
        data = self.slo.getNotionalValues(['ETHBTC','BNBBTC','XRPBTC'],5)
        
        assert data.loc['ETHBTC']['totalNotionalBids'] == 120.02999999999999
        
        assert data.loc['BNBBTC']['totalNotionalAsks'] == 121.86
        
        assert data.loc['XRPBTC']['totalNotionalBids'] == 0.0        

    @mock.patch('spreadLogger.spreadLogger.getOrderData',return_value=mock_data_2)
    def test_getSpreads(self,mock_args):
        
        data = self.slo.getSpreads(['ETHBTC','BNBBTC','XRPBTC'])
        
        assert data.loc['ETHBTC']['spreads'] == [5.07, 0.05999999999999983, 3.9899999999999998, 3.719999999999999, 5.569999999999999, 0.0, 1.4799999999999995, 5.01, 3.879999999999999, 1.5499999999999998, 2.68, 1.8099999999999996]
        
        assert data.loc['XRPBTC']['spreads'] == [0.0]
        
        return

    @mock.patch('spreadLogger.spreadLogger.getOrderData',return_value=mock_data_3)
    def test_produceDeltas(self,mock_args):
        self.slo.spreadData = pd.concat([self.slo.spreadData,self.pdData])
        
        latest = self.slo.getSpreads(['TRXUSDT', 'LTCUSDT', 'BNBUSDT', 'ETHUSDT', 'BTCUSDT'])
        
        deltas = self.slo.produceDeltas('BNBUSDT',latest)
        
        assert deltas == [3,3,3,2,2]
        
        assert self.slo.spreadData.loc['BNBUSDT']['spreads'] == [3,3,5,3,2]
        
        deltas = self.slo.produceDeltas('TRXUSDT',latest)
        
        assert deltas == [1,7,2,5,6]
        
        assert self.slo.spreadData.loc['TRXUSDT']['spreads'] == [1,7,2,5,6]
        
        deltas = self.slo.produceDeltas('ETHUSDT',latest)
        
        assert deltas == [6,4,1,7,2]
        
        assert self.slo.spreadData.loc['ETHUSDT']['spreads'] == [1,2,3,8,1]
        
        deltas = self.slo.produceDeltas('LTCUSDT',latest)
        
        assert deltas == [2,9,7,2,3]
        
        assert self.slo.spreadData.loc['LTCUSDT']['spreads'] == [2,9,7,2,3]
