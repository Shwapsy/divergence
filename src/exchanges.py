import ccxt.pro as ccxtpro
import asyncio
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)

class ExchangeManager:
    def __init__(self):
        self.binance_spot = None
        self.binance_futures = None
        self.bybit_spot = None
        self.bybit_futures = None
        self.gate_spot = None
        self.gate_futures = None
        
        # Кэш цен (обновляется через WebSocket)
        self.spot_prices: Dict[str, Dict[str, float]] = {
            'binance': {},
            'bybit': {},
            'gate': {}
        }
        self.futures_prices: Dict[str, Dict[str, float]] = {
            'binance': {},
            'bybit': {},
            'gate': {}
        }
        
        self._running = False
        self._tasks = []
        
    def _normalize_coin_name(self, coin: str) -> str:
        coin = coin.upper()
        if coin.startswith('1000'):
            coin = coin[4:]
        if coin.startswith('10000'):
            coin = coin[5:]
        return coin
    
    async def _init_exchanges(self):
        """Инициализация всех бирж"""
        if not self.binance_spot:
            self.binance_spot = ccxtpro.binance({
                'enableRateLimit': True,
                'newUpdates': True
            })
        if not self.binance_futures:
            self.binance_futures = ccxtpro.binance({
                'enableRateLimit': True,
                'newUpdates': True,
                'options': {'defaultType': 'future'}
            })
        if not self.bybit_spot:
            self.bybit_spot = ccxtpro.bybit({
                'enableRateLimit': True,
                'newUpdates': True
            })
        if not self.bybit_futures:
            self.bybit_futures = ccxtpro.bybit({
                'enableRateLimit': True,
                'newUpdates': True,
                'options': {'defaultType': 'swap'}
            })
        if not self.gate_spot:
            self.gate_spot = ccxtpro.gate({
                'enableRateLimit': True,
                'newUpdates': True
            })
        if not self.gate_futures:
            self.gate_futures = ccxtpro.gate({
                'enableRateLimit': True,
                'newUpdates': True,
                'options': {'defaultType': 'swap'}
            })
    
    async def _watch_tickers(self, exchange, exchange_name: str, market_type: str):
        """Подписка на все тикеры биржи"""
        prices_dict = self.spot_prices if market_type == 'spot' else self.futures_prices
        
        while self._running:
            try:
                # watch_tickers подписывается на все доступные тикеры
                tickers = await exchange.watch_tickers()
                
                for symbol, ticker in tickers.items():
                    if ticker.get('last') and 'USDT' in symbol:
                        # Извлекаем базовую валюту
                        base = symbol.split('/')[0] if '/' in symbol else symbol.replace('USDT', '')
                        base = self._normalize_coin_name(base)
                        
                        if base and 'UP' not in base and 'DOWN' not in base:
                            prices_dict[exchange_name][base] = float(ticker['last'])
                            
            except Exception as e:
                if '403' in str(e) or 'Forbidden' in str(e):
                    logger.error(f"{exchange_name} {market_type} blocked from this region")
                    break
                logger.error(f"Error watching {exchange_name} {market_type}: {e}")
                await asyncio.sleep(5)
    
    async def start_websockets(self):
        """Запуск всех WebSocket соединений"""
        await self._init_exchanges()
        self._running = True
        
        logger.info("Starting WebSocket connections...")
        
        self._tasks = [
            asyncio.create_task(self._watch_tickers(self.binance_spot, 'binance', 'spot')),
            asyncio.create_task(self._watch_tickers(self.binance_futures, 'binance', 'futures')),
            asyncio.create_task(self._watch_tickers(self.bybit_spot, 'bybit', 'spot')),
            asyncio.create_task(self._watch_tickers(self.bybit_futures, 'bybit', 'futures')),
            asyncio.create_task(self._watch_tickers(self.gate_spot, 'gate', 'spot')),
            asyncio.create_task(self._watch_tickers(self.gate_futures, 'gate', 'futures')),
        ]
        
        logger.info("WebSocket connections started")
    
    async def stop_websockets(self):
        """Остановка всех WebSocket соединений"""
        self._running = False
        
        for task in self._tasks:
            task.cancel()
        
        # Закрываем соединения
        exchanges = [
            self.binance_spot, self.binance_futures,
            self.bybit_spot, self.bybit_futures,
            self.gate_spot, self.gate_futures
        ]
        for ex in exchanges:
            if ex:
                try:
                    await ex.close()
                except:
                    pass
    
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
    
    def get_binance_data(self) -> List[Tuple[str, float, float, float]]:
        """Получение данных Binance из кэша (синхронный метод)"""
        return self._calculate_deviations(
            self.spot_prices['binance'],
            self.futures_prices['binance']
        )
    
    def get_bybit_data(self) -> List[Tuple[str, float, float, float]]:
        """Получение данных Bybit из кэша (синхронный метод)"""
        return self._calculate_deviations(
            self.spot_prices['bybit'],
            self.futures_prices['bybit']
        )
    
    def get_gate_data(self) -> List[Tuple[str, float, float, float]]:
        """Получение данных Gate из кэша (синхронный метод)"""
        return self._calculate_deviations(
            self.spot_prices['gate'],
            self.futures_prices['gate']
        )
    
    async def get_all_deviations(self, binance_enabled: bool = True, 
                                  bybit_enabled: bool = True,
                                  gate_enabled: bool = True) -> Dict[str, List[Tuple[str, float, float, float]]]:
        """Получение всех отклонений из кэша"""
        results = {}
        
        if binance_enabled:
            results['binance'] = self.get_binance_data()
            logger.info(f"Binance: {len(results['binance'])} deviations from cache")
        
        if bybit_enabled:
            results['bybit'] = self.get_bybit_data()
            logger.info(f"Bybit: {len(results['bybit'])} deviations from cache")
        
        if gate_enabled:
            results['gate'] = self.get_gate_data()
            logger.info(f"Gate: {len(results['gate'])} deviations from cache")
        
        return results

exchange_manager = ExchangeManager()
