import ccxt
import asyncio
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class ExchangeManager:
    def __init__(self):
        self.binance_spot = ccxt.binance({'enableRateLimit': True})
        self.binance_futures = ccxt.binanceusdm({'enableRateLimit': True})
        self.bybit_spot = ccxt.bybit({'enableRateLimit': True})
        self.bybit_futures = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
        self.gate_spot = ccxt.gate({'enableRateLimit': True})
        self.gate_futures = ccxt.gate({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
        
    def _normalize_coin_name(self, coin: str) -> str:
        coin = coin.upper()
        if coin.startswith('1000'):
            coin = coin[4:]
        if coin.startswith('10000'):
            coin = coin[5:]
        return coin
    
    def _time_until_funding(self, next_funding_time: Optional[int]) -> str:
        if not next_funding_time:
            return "8h"
        now_ms = int(datetime.now().timestamp() * 1000)
        diff_ms = next_funding_time - now_ms
        if diff_ms <= 0:
            return "0m"
        hours = diff_ms // (1000 * 60 * 60)
        minutes = (diff_ms % (1000 * 60 * 60)) // (1000 * 60)
        if hours > 0:
            return f"{hours}h{minutes}m" if minutes > 0 else f"{hours}h"
        return f"{minutes}m"
        
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
    
    async def get_binance_funding_rates(self) -> Dict[str, Tuple[float, str]]:
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.binance_futures.load_markets)
            
            funding_rates = await loop.run_in_executor(
                None, 
                lambda: self.binance_futures.fetch_funding_rates()
            )
            
            result = {}
            for symbol, data in funding_rates.items():
                if symbol in self.binance_futures.markets:
                    market = self.binance_futures.markets[symbol]
                    if market.get('settle') == 'USDT' and market.get('linear', False):
                        base = market.get('base', '')
                        if base:
                            base = self._normalize_coin_name(base)
                            rate = data.get('fundingRate', 0) or 0
                            next_time = data.get('fundingTimestamp')
                            time_str = self._time_until_funding(next_time)
                            result[base] = (rate * 100, time_str)
            
            logger.info(f"Binance: {len(result)} funding rates fetched")
            return result
        except Exception as e:
            logger.error(f"Error fetching Binance funding rates: {e}")
            return {}
    
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
    
    async def get_bybit_funding_rates(self) -> Dict[str, Tuple[float, str]]:
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.bybit_futures.load_markets)
            
            funding_rates = await loop.run_in_executor(
                None, 
                lambda: self.bybit_futures.fetch_funding_rates(params={'type': 'swap'})
            )
            
            result = {}
            for symbol, data in funding_rates.items():
                if symbol in self.bybit_futures.markets:
                    market = self.bybit_futures.markets[symbol]
                    if market.get('settle') == 'USDT' and market.get('type') == 'swap':
                        base = market.get('base', '')
                        if base:
                            base = self._normalize_coin_name(base)
                            rate = data.get('fundingRate', 0) or 0
                            next_time = data.get('fundingTimestamp')
                            time_str = self._time_until_funding(next_time)
                            result[base] = (rate * 100, time_str)
            
            logger.info(f"Bybit: {len(result)} funding rates fetched")
            return result
        except Exception as e:
            logger.error(f"Error fetching Bybit funding rates: {e}")
            return {}
    
    async def get_gate_prices(self) -> Tuple[Dict[str, float], Dict[str, float]]:
        try:
            loop = asyncio.get_event_loop()
            
            await loop.run_in_executor(None, self.gate_spot.load_markets)
            await loop.run_in_executor(None, self.gate_futures.load_markets)
            
            logger.info(f"Gate markets loaded: spot={len(self.gate_spot.markets)}, futures={len(self.gate_futures.markets)}")
            
            spot_tickers = await loop.run_in_executor(None, self.gate_spot.fetch_tickers)
            futures_tickers = await loop.run_in_executor(None, self.gate_futures.fetch_tickers)
            
            logger.info(f"Gate tickers fetched: spot={len(spot_tickers)}, futures={len(futures_tickers)}")
            
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
            
            logger.info(f"Gate: {len(spot_prices)} spot, {len(futures_prices)} futures pairs after filtering")
            return spot_prices, futures_prices
        except Exception as e:
            logger.error(f"Error fetching Gate prices: {e}", exc_info=True)
            return {}, {}
    
    async def get_gate_funding_rates(self) -> Dict[str, Tuple[float, str]]:
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.gate_futures.load_markets)
            
            funding_rates = await loop.run_in_executor(
                None, 
                lambda: self.gate_futures.fetch_funding_rates()
            )
            
            result = {}
            for symbol, data in funding_rates.items():
                if symbol in self.gate_futures.markets:
                    market = self.gate_futures.markets[symbol]
                    if market.get('settle') == 'USDT' and market.get('type') == 'swap':
                        base = market.get('base', '')
                        if base:
                            base = self._normalize_coin_name(base)
                            rate = data.get('fundingRate', 0) or 0
                            next_time = data.get('fundingTimestamp')
                            time_str = self._time_until_funding(next_time)
                            result[base] = (rate * 100, time_str)
            
            logger.info(f"Gate: {len(result)} funding rates fetched")
            return result
        except Exception as e:
            logger.error(f"Error fetching Gate funding rates: {e}")
            return {}
    
    def calculate_deviations(self, spot_prices: Dict[str, float], 
                            futures_prices: Dict[str, float],
                            funding_rates: Dict[str, Tuple[float, str]]) -> List[Tuple[str, float, float, float, float, str]]:
        deviations = []
        common_coins = set(spot_prices.keys()) & set(futures_prices.keys())
        
        for coin in common_coins:
            spot = spot_prices[coin]
            futures = futures_prices[coin]
            if spot > 0 and futures > 0:
                deviation = ((futures - spot) / spot) * 100
                if abs(deviation) <= 20.0:
                    fr_rate, fr_time = funding_rates.get(coin, (0.0, "8h"))
                    deviations.append((coin, spot, futures, deviation, fr_rate, fr_time))
                else:
                    logger.warning(f"Large deviation filtered: {coin} spot={spot} futures={futures} dev={deviation:.2f}%")
        
        deviations.sort(key=lambda x: abs(x[3]), reverse=True)
        return deviations
    
    async def get_all_deviations(self, binance_enabled: bool = True, 
                                  bybit_enabled: bool = True,
                                  gate_enabled: bool = True) -> Dict[str, List[Tuple[str, float, float, float, float, str]]]:
        results = {}
        price_tasks = []
        funding_tasks = []
        task_names = []
        
        if binance_enabled:
            price_tasks.append(self.get_binance_prices())
            funding_tasks.append(self.get_binance_funding_rates())
            task_names.append('binance')
        
        if bybit_enabled:
            price_tasks.append(self.get_bybit_prices())
            funding_tasks.append(self.get_bybit_funding_rates())
            task_names.append('bybit')
        
        if gate_enabled:
            price_tasks.append(self.get_gate_prices())
            funding_tasks.append(self.get_gate_funding_rates())
            task_names.append('gate')
        
        if price_tasks:
            all_tasks = price_tasks + funding_tasks
            fetched = await asyncio.gather(*all_tasks, return_exceptions=True)
            
            num_exchanges = len(task_names)
            price_results = fetched[:num_exchanges]
            funding_results = fetched[num_exchanges:]
            
            for i, name in enumerate(task_names):
                price_result = price_results[i]
                funding_result = funding_results[i]
                
                if isinstance(price_result, Exception):
                    logger.error(f"Error fetching {name} prices: {price_result}")
                    results[name] = []
                else:
                    spot, futures = price_result
                    
                    if isinstance(funding_result, Exception):
                        logger.error(f"Error fetching {name} funding: {funding_result}")
                        funding_rates = {}
                    else:
                        funding_rates = funding_result
                    
                    deviations = self.calculate_deviations(spot, futures, funding_rates)
                    logger.info(f"{name}: {len(deviations)} deviations calculated")
                    results[name] = deviations
        
        return results

exchange_manager = ExchangeManager()
