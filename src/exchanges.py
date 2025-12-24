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
        
    def _normalize_coin_name(self, coin: str) -> str:
        coin = coin.upper()
        if coin.startswith('1000'):
            coin = coin[4:]
        if coin.startswith('10000'):
            coin = coin[5:]
        return coin
        
    async def get_binance_prices(self) -> Tuple[Dict[str, float], Dict[str, float]]:
        try:
            loop = asyncio.get_event_loop()
            
            await loop.run_in_executor(None, self.binance_spot.load_markets)
            await loop.run_in_executor(None, self.binance_futures.load_markets)
            
            spot_tickers = await loop.run_in_executor(None, self.binance_spot.fetch_tickers)
            futures_tickers = await loop.run_in_executor(None, self.binance_futures.fetch_tickers)
            
            spot_prices = {}
            for symbol, ticker in spot_tickers.items():
                if symbol in self.binance_spot.markets:
                    market = self.binance_spot.markets[symbol]
                    if market.get('quote') == 'USDT' and market.get('active', True):
                        base = market.get('base', '')
                        if ticker.get('last') and base:
                            base = self._normalize_coin_name(base)
                            price = float(ticker['last'])
                            if price > 0 and 'UP' not in base and 'DOWN' not in base:
                                spot_prices[base] = price
            
            futures_prices = {}
            for symbol, ticker in futures_tickers.items():
                if symbol in self.binance_futures.markets:
                    market = self.binance_futures.markets[symbol]
                    if market.get('settle') == 'USDT' and market.get('active', True) and market.get('linear', False):
                        base = market.get('base', '')
                        if ticker.get('last') and base:
                            base = self._normalize_coin_name(base)
                            price = float(ticker['last'])
                            if price > 0:
                                futures_prices[base] = price
            
            logger.info(f"Binance: {len(spot_prices)} spot, {len(futures_prices)} futures pairs")
            return spot_prices, futures_prices
        except Exception as e:
            logger.error(f"Error fetching Binance prices: {e}")
            return {}, {}
    
    async def get_bybit_prices(self) -> Tuple[Dict[str, float], Dict[str, float]]:
        try:
            loop = asyncio.get_event_loop()
            
            try:
                await loop.run_in_executor(None, self.bybit_spot.load_markets)
                await loop.run_in_executor(None, self.bybit_futures.load_markets)
            except Exception as e:
                if '403' in str(e) or 'Forbidden' in str(e):
                    logger.error("Bybit API blocked from this region (403 Forbidden)")
                    return {}, {}
                raise
            
            logger.info(f"Bybit markets loaded: spot={len(self.bybit_spot.markets)}, futures={len(self.bybit_futures.markets)}")
            
            spot_tickers = await loop.run_in_executor(None, lambda: self.bybit_spot.fetch_tickers(params={'type': 'spot'}))
            futures_tickers = await loop.run_in_executor(None, lambda: self.bybit_futures.fetch_tickers(params={'type': 'swap'}))
            
            logger.info(f"Bybit tickers fetched: spot={len(spot_tickers)}, futures={len(futures_tickers)}")
            
            spot_prices = {}
            spot_types_seen = set()
            for symbol, ticker in spot_tickers.items():
                if symbol in self.bybit_spot.markets:
                    market = self.bybit_spot.markets[symbol]
                    quote = market.get('quote')
                    active = market.get('active')
                    mtype = market.get('type')
                    spot_flag = market.get('spot')
                    
                    if quote == 'USDT':
                        spot_types_seen.add(f"type={mtype},spot={spot_flag}")
                    
                    if quote == 'USDT' and active is not False and (mtype == 'spot' or spot_flag == True):
                        base = market.get('base', '')
                        if ticker.get('last') and base:
                            base = self._normalize_coin_name(base)
                            price = float(ticker['last'])
                            if price > 0 and 'UP' not in base and 'DOWN' not in base:
                                spot_prices[base] = price
            
            if spot_types_seen:
                logger.info(f"Bybit spot market types seen: {list(spot_types_seen)[:5]}")
            
            futures_prices = {}
            for symbol, ticker in futures_tickers.items():
                if symbol in self.bybit_futures.markets:
                    market = self.bybit_futures.markets[symbol]
                    settle = market.get('settle')
                    active = market.get('active')
                    mtype = market.get('type')
                    
                    if settle == 'USDT' and active is not False and mtype == 'swap':
                        base = market.get('base', '')
                        if ticker.get('last') and base:
                            base = self._normalize_coin_name(base)
                            price = float(ticker['last'])
                            if price > 0:
                                futures_prices[base] = price
            
            logger.info(f"Bybit: {len(spot_prices)} spot, {len(futures_prices)} futures pairs after filtering")
            return spot_prices, futures_prices
        except Exception as e:
            logger.error(f"Error fetching Bybit prices: {e}", exc_info=True)
            return {}, {}
    
    def calculate_deviations(self, spot_prices: Dict[str, float], 
                            futures_prices: Dict[str, float]) -> List[Tuple[str, float, float, float]]:
        deviations = []
        common_coins = set(spot_prices.keys()) & set(futures_prices.keys())
        
        for coin in common_coins:
            spot = spot_prices[coin]
            futures = futures_prices[coin]
            if spot > 0 and futures > 0:
                deviation = ((futures - spot) / spot) * 100
                if abs(deviation) <= 20.0:
                    deviations.append((coin, spot, futures, deviation))
                else:
                    logger.warning(f"Large deviation filtered: {coin} spot={spot} futures={futures} dev={deviation:.2f}%")
        
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
                    deviations = self.calculate_deviations(spot, futures)
                    logger.info(f"{name}: {len(deviations)} deviations calculated")
                    results[name] = deviations
        
        return results

exchange_manager = ExchangeManager()
