from spreadLogger import *
from spreadCollector import *

if __name__ == "__main__":
    t = spreadLogger(5,'USDT',key='count',lim=100)
    
    #should produce the results for Q1
    logging.info("----- Question 1 Output ------")
    top_volumes = t.topQuoteAsset(quoteAsset='BTC',key='volume')
    
    #should produce the results for Q2
    logging.info("----- Question 2 Output ------")
    top_trades = t.topQuoteAsset()
    
    t.limit = 5000 #set the limit to max
    
    #should produce the results for Q3
    logging.info("----- Question 3 Output ------")
    logging.info(t.getNotionalValues(top_volumes,200))
    logging.info("")

    #should produce the results for Q4
    t.limit = 1
    logging.info("----- Question 4 Output ------")
    logging.info(t.getSpreads(top_trades))
    logging.info("")
    
    
    #should produce the results for Q5
    logging.info("----- Question 5 Output Start ------")
    t.start()
    
    time.sleep(60)
    
    t.stopit()
    logging.info("----- Question 5 Output Finish ------")

    start_http_server(8080)
    sLogger = spreadLogger(5,'USDT',key='count',lim=1,base_URL="https://testnet.binance.vision")
    REGISTRY.register(SpreadCollector(sLogger))
    while True:
        time.sleep(2)
