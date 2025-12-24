import ccxt
import asyncio
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)

class ExchangeManager:
    def __init__(self):
        self.binance_spot = ccxt.binance({'enableRateLimit': True})
        self.binance_futures = ccxt.binanceusdm({'enableRateLimit': True})
        self.bybit_spot = ccxt.bybit({'enableRateLimit': True})
        self.bybit_futures = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
        
    async def get_binance_prices(self) -> Tuple[Dict[str, float], Dict[str, float]]:
        try:
            loop = asyncio.get_event_loop()
            spot_tickers = await loop.run_in_executor(None, self.binance_spot.fetch_tickers)
            futures_tickers = await loop.run_in_executor(None, self.binance_futures.fetch_tickers)
            
            spot_prices = {}
            for symbol, ticker in spot_tickers.items():
                if '/USDT' in symbol and ticker.get('last'):
                    base = symbol.split('/')[0]
                    spot_prices[base] = float(ticker['last'])
            
            futures_prices = {}
            for symbol, ticker in futures_tickers.items():
                if '/USDT' in symbol and ticker.get('last'):
                    base = symbol.split('/')[0].replace(':USDT', '')
                    futures_prices[base] = float(ticker['last'])
            
            return spot_prices, futures_prices
        except Exception as e:
            logger.error(f"Error fetching Binance prices: {e}")
            return {}, {}
    
    async def get_bybit_prices(self) -> Tuple[Dict[str, float], Dict[str, float]]:
        try:
            loop = asyncio.get_event_loop()
            spot_tickers = await loop.run_in_executor(None, self.bybit_spot.fetch_tickers)
            futures_tickers = await loop.run_in_executor(None, self.bybit_futures.fetch_tickers)
            
            spot_prices = {}
            for symbol, ticker in spot_tickers.items():
                if '/USDT' in symbol and ticker.get('last'):
                    base = symbol.split('/')[0]
                    spot_prices[base] = float(ticker['last'])
            
            futures_prices = {}
            for symbol, ticker in futures_tickers.items():
                if '/USDT' in symbol and ticker.get('last'):
                    base = symbol.split('/')[0].replace(':USDT', '')
                    futures_prices[base] = float(ticker['last'])
            
            return spot_prices, futures_prices
        except Exception as e:
            logger.error(f"Error fetching Bybit prices: {e}")
            return {}, {}
    
    def calculate_deviations(self, spot_prices: Dict[str, float], 
                            futures_prices: Dict[str, float]) -> List[Tuple[str, float, float, float]]:
        deviations = []
        common_coins = set(spot_prices.keys()) & set(futures_prices.keys())
        
        for coin in common_coins:
            spot = spot_prices[coin]
            futures = futures_prices[coin]
            if spot > 0:
                deviation = ((futures - spot) / spot) * 100
                deviations.append((coin, spot, futures, deviation))
        
        deviations.sort(key=lambda x: abs(x[3]), reverse=True)
        return deviations
    
    async def get_all_deviations(self, binance_enabled: bool = True, 
                                  bybit_enabled: bool = True) -> Dict[str, List[Tuple[str, float, float, float]]]:
        results = {}
        tasks = []
        task_names = []
        
        if binance_enabled:
            tasks.append(self.get_binance_prices())
            task_names.append('binance')
        
        if bybit_enabled:
            tasks.append(self.get_bybit_prices())
            task_names.append('bybit')
        
        if tasks:
            fetched = await asyncio.gather(*tasks, return_exceptions=True)
            for name, result in zip(task_names, fetched):
                if isinstance(result, Exception):
                    logger.error(f"Error fetching {name}: {result}")
                    results[name] = []
                else:
                    spot, futures = result
                    results[name] = self.calculate_deviations(spot, futures)
        
        return results

exchange_manager = ExchangeManager()
