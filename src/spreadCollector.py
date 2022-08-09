from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily,REGISTRY

class SpreadCollector(object):
    def __init__(self,spreadLoggerObj):
        self.slo = spreadLoggerObj
    
    def collect(self):
        gauge = GaugeMetricFamily("spread_deltas_2", "spread deltas per sym", labels=["sym"])
        
        try:
            top_assets = self.slo.topQuoteAsset()
            if not top_assets:
                yield

            df = self.slo.getSpreads(top_assets)
            if 'symbol' != df.index.name:
                yield

            for sym in df.index.tolist():
                deltas = self.slo.produceDeltas(sym,df)
                gauge.add_metric([sym],deltas[0])
                
        except Exception as e:
            logging.error("failed to add metric due to error : {}".format(e))
            
        yield gauge
