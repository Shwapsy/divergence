import ccxt
import asyncio
from typing import Dict, List, Tuple
import logging
import gc

logger = logging.getLogger(__name__)

class ExchangeManager:
    def __init__(self):
        self.binance_spot = None
        self.binance_futures = None
        self.bybit_spot = None
        self.bybit_futures = None
        self.gate_spot = None
        self.gate_futures = None
        
    def _normalize_coin_name(self, coin: str) -> str:
        coin = coin.upper()
        if coin.startswith('1000'):
            coin = coin[4:]
        if coin.startswith('10000'):
            coin = coin[5:]
        return coin
    
    def _init_exchange(self, name: str):
        if name == 'binance_spot' and not self.binance_spot:
            self.binance_spot = ccxt.binance({
                'enableRateLimit': True,
                'hostname': 'data-api.binance.vision'
            })
        elif name == 'binance_futures' and not self.binance_futures:
            self.binance_futures = ccxt.binanceusdm({
                'enableRateLimit': True,
                'hostname': 'fapi.binance.com'
            })
        elif name == 'bybit_spot' and not self.bybit_spot:
            self.bybit_spot = ccxt.bybit({'enableRateLimit': True})
        elif name == 'bybit_futures' and not self.bybit_futures:
            self.bybit_futures = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
        elif name == 'gate_spot' and not self.gate_spot:
            self.gate_spot = ccxt.gate({'enableRateLimit': True})
        elif name == 'gate_futures' and not self.gate_futures:
            self.gate_futures = ccxt.gate({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
        
    async def get_binance_data(self) -> List[Tuple[str, float, float, float]]:
        try:
            loop = asyncio.get_event_loop()
            
            self._init_exchange('binance_spot')
            self._init_exchange('binance_futures')
            
            await loop.run_in_executor(None, self.binance_spot.load_markets)
            await loop.run_in_executor(None, self.binance_futures.load_markets)
            
            spot_tickers = await loop.run_in_executor(None, self.binance_spot.fetch_tickers)
            
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
            
            del spot_tickers
            gc.collect()
            
            futures_tickers = await loop.run_in_executor(None, self.binance_futures.fetch_tickers)
            
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
            
            del futures_tickers
            gc.collect()
            
            deviations = self._calculate_deviations(spot_prices, futures_prices)
            
            del spot_prices, futures_prices
            gc.collect()
            
            logger.info(f"Binance: {len(deviations)} deviations calculated")
            return deviations
        except Exception as e:
            logger.error(f"Error fetching Binance data: {e}")
            return []
    
    async def get_bybit_data(self) -> List[Tuple[str, float, float, float]]:
        try:
            loop = asyncio.get_event_loop()
            
            self._init_exchange('bybit_spot')
            self._init_exchange('bybit_futures')
            
            try:
                await loop.run_in_executor(None, self.bybit_spot.load_markets)
                await loop.run_in_executor(None, self.bybit_futures.load_markets)
            except Exception as e:
                if '403' in str(e) or 'Forbidden' in str(e):
                    logger.error("Bybit API blocked from this region (403 Forbidden)")
                    return []
                raise
            
            spot_tickers = await loop.run_in_executor(None, lambda: self.bybit_spot.fetch_tickers(params={'type': 'spot'}))
            
            spot_prices = {}
            for symbol, ticker in spot_tickers.items():
                if symbol in self.bybit_spot.markets:
                    market = self.bybit_spot.markets[symbol]
                    quote = market.get('quote')
                    active = market.get('active')
                    mtype = market.get('type')
                    spot_flag = market.get('spot')
                    
                    if quote == 'USDT' and active is not False and (mtype == 'spot' or spot_flag == True):
                        base = market.get('base', '')
                        if ticker.get('last') and base:
                            base = self._normalize_coin_name(base)
                            price = float(ticker['last'])
                            if price > 0 and 'UP' not in base and 'DOWN' not in base:
                                spot_prices[base] = price
            
            del spot_tickers
            gc.collect()
            
            futures_tickers = await loop.run_in_executor(None, lambda: self.bybit_futures.fetch_tickers(params={'type': 'swap'}))
            
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
            
            del futures_tickers
            gc.collect()
            
            deviations = self._calculate_deviations(spot_prices, futures_prices)
            
            del spot_prices, futures_prices
            gc.collect()
            
            logger.info(f"Bybit: {len(deviations)} deviations calculated")
            return deviations
        except Exception as e:
            logger.error(f"Error fetching Bybit data: {e}", exc_info=True)
            return []
    
    async def get_gate_data(self) -> List[Tuple[str, float, float, float]]:
        try:
            loop = asyncio.get_event_loop()
            
            self._init_exchange('gate_spot')
            self._init_exchange('gate_futures')
            
            await loop.run_in_executor(None, self.gate_spot.load_markets)
            await loop.run_in_executor(None, self.gate_futures.load_markets)
            
            spot_tickers = await loop.run_in_executor(None, self.gate_spot.fetch_tickers)
            
            spot_prices = {}
            for symbol, ticker in spot_tickers.items():
                if symbol in self.gate_spot.markets:
                    market = self.gate_spot.markets[symbol]
                    quote = market.get('quote')
                    active = market.get('active')
                    mtype = market.get('type')
                    spot_flag = market.get('spot')
                    
                    if quote == 'USDT' and active is not False and (mtype == 'spot' or spot_flag == True):
                        base = market.get('base', '')
                        if ticker.get('last') and base:
                            base = self._normalize_coin_name(base)
                            price = float(ticker['last'])
                            if price > 0 and 'UP' not in base and 'DOWN' not in base:
                                spot_prices[base] = price
            
            del spot_tickers
            gc.collect()
            
            futures_tickers = await loop.run_in_executor(None, self.gate_futures.fetch_tickers)
            
            futures_prices = {}
            for symbol, ticker in futures_tickers.items():
                if symbol in self.gate_futures.markets:
                    market = self.gate_futures.markets[symbol]
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
            
            del futures_tickers
            gc.collect()
            
            deviations = self._calculate_deviations(spot_prices, futures_prices)
            
            del spot_prices, futures_prices
            gc.collect()
            
            logger.info(f"Gate: {len(deviations)} deviations calculated")
            return deviations
        except Exception as e:
            logger.error(f"Error fetching Gate data: {e}", exc_info=True)
            return []
    
    def _calculate_deviations(self, spot_prices: Dict[str, float], 
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
        
        deviations.sort(key=lambda x: abs(x[3]), reverse=True)
        return deviations
    
    async def get_all_deviations(self, binance_enabled: bool = True, 
                                  bybit_enabled: bool = True,
                                  gate_enabled: bool = True) -> Dict[str, List[Tuple[str, float, float, float]]]:
        results = {}
        
        if binance_enabled:
            results['binance'] = await self.get_binance_data()
            gc.collect()
        
        if bybit_enabled:
            results['bybit'] = await self.get_bybit_data()
            gc.collect()
        
        if gate_enabled:
            results['gate'] = await self.get_gate_data()
            gc.collect()
        
        return results

exchange_manager = ExchangeManager()
