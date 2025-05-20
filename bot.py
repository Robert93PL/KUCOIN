import ccxt
import pandas as pd
import numpy as np
import pandas_ta as ta
import os
import time
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
from colorama import init, Fore, Style
import json
import warnings
from pymongo import MongoClient

warnings.filterwarnings('ignore')

# Inicjalizacja colorama
init()

# Konfiguracja logowania z rotacją
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler('trade_log.txt', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)

# Konfiguracja MongoDB
MONGO_URI = os.getenv('MONGO_URI', 'your_mongodb_connection_string')
client = MongoClient(MONGO_URI)
db = client['trading_bot']
positions_collection = db['positions']

# Konfiguracja API KuCoin
exchanges = [
    ccxt.kucoin({
        'apiKey': '6823adfb61d4190001722ff5',
        'secret': '5ec6154e-0753-4845-b391-4e95f6ba4b74',
        'password': 'robertai',
        'enableRateLimit': True
    }),
    ccxt.kucoin({
        'apiKey': '6823b020c058ba0001f9f3e5',
        'secret': 'c2ce86aa-5fd0-4a44-b675-8ba7f9671a76',
        'password': 'robertai',
        'enableRateLimit': True
    }),
    ccxt.kucoin({
        'apiKey': '6823b05a4985e300012f7e42',
        'secret': '1d4b21df-3237-4471-82d0-fb140ed764d0',
        'password': 'robertai',
        'enableRateLimit': True
    })
]

for i, exchange in enumerate(exchanges):
    exchange.exchange_id = f'KuCoin{i+1}'

# Parametry handlu
timeframe = '1h'
initial_capital = 70
max_positions = 10
capital_per_position = 6
fee_rate = 0.001
data_days = 7
scan_interval = 180
sl_tp_interval = 3
price_update_interval = 10

# Strategie
strategies = [
    {
        'name': 'Strategia 45',
        'indicators': ['rsi', 'macd', 'bollinger'],
        'params': {
            'rsi_period': 7, 'rsi_low': 30, 'rsi_high': 85,
            'macd_fast': 12, 'macd_slow': 26, 'macd_signal': 9,
            'bb_period': 10, 'bb_std': 1.0,
            'sl_percent': 3.0, 'tp_percent': 4.0
        }
    },
    # Dodaj pozostałe strategie
]

async def place_limit_order(symbol, side, amount, price, exchange_index=0, retries=3, delay=5):
    exchange = exchanges[exchange_index % len(exchanges)]
    for attempt in range(retries):
        try:
            markets = exchange.load_markets()
            market = markets[symbol]
            price_precision = market['precision']['price']
            price = round(price, price_precision)
            amount = round(amount, market['precision']['amount'])
            order = exchange.create_limit_order(symbol, side, amount, price)
            logging.info(f"Zlecenie limitowe {side} dla {symbol}: ilość={amount}, cena={price}, order_id={order['id']} ({exchange.exchange_id})")
            return order
        except ccxt.RateLimitExceeded as e:
            logging.warning(f"Limit API osiągnięty dla {symbol} ({exchange.exchange_id}). Próba {attempt + 1}/{retries}. Czekam {delay} sekund.")
            await asyncio.sleep(delay)
            delay *= 2
        except Exception as e:
            logging.error(f"Błąd przy składaniu zlecenia limitowego {side} dla {symbol} ({exchange.exchange_id}): {e}")
            return None
    logging.error(f"Nie udało się złożyć zlecenia po {retries} próbach dla {symbol} ({exchange.exchange_id})")
    return None

async def cancel_order(symbol, order_id, exchange_index=0):
    exchange = exchanges[exchange_index % len(exchanges)]
    try:
        exchange.cancel_order(order_id, symbol)
        logging.info(f"Anulowano zlecenie {order_id} dla {symbol} ({exchange.exchange_id})")
    except Exception as e:
        logging.error(f"Błąd przy anulowaniu zlecenia {order_id} dla {symbol} ({exchange.exchange_id}): {e}")

def save_positions(positions):
    try:
        positions_to_save = []
        for key, pos in positions.items():
            pos_copy = pos.copy()
            pos_copy['buy_time'] = pos_copy['buy_time'].isoformat()
            pos_copy['strategy'] = pos_copy['strategy']['name']
            pos_copy['symbol'] = key.split('_', 1)[0] if '_' in key else key
            pos_copy['tp_order_id'] = pos.get('tp_order_id')
            pos_copy['sl_order_id'] = pos.get('sl_order_id')
            pos_copy['_id'] = key
            positions_to_save.append(pos_copy)
        if positions_to_save:
            positions_collection.delete_many({})
            positions_collection.insert_many(positions_to_save)
        else:
            positions_collection.delete_many({})
        logging.info("Zapisano stan pozycji do MongoDB")
    except Exception as e:
        logging.error(f"Błąd przy zapisywaniu pozycji do MongoDB: {e}")

def load_positions():
    try:
        positions = {}
        for pos in positions_collection.find():
            strategy = next((s for s in strategies if s['name'] == pos['strategy']), strategies[0])
            key = pos['_id']
            positions[key] = {
                'amount': pos['amount'],
                'entry_price': pos['entry_price'],
                'strategy': strategy,
                'buy_time': datetime.fromisoformat(pos['buy_time']),
                'current_price': pos.get('current_price', pos['entry_price']),
                'tp_price': pos.get('tp_price', pos['entry_price'] * (1 + strategy['params']['tp_percent'] / 100)),
                'sl_price': pos.get('sl_price', pos['entry_price'] * (1 - strategy['params']['sl_percent'] / 100)),
                'tp_order_id': pos.get('tp_order_id'),
                'sl_order_id': pos.get('sl_order_id')
            }
        logging.info(f"Wczytano {len(positions)} pozycji z MongoDB")
        return positions
    except Exception as e:
        logging.error(f"Błąd przy wczytywaniu pozycji z MongoDB: {e}")
        return {}

# Funkcje get_top_pairs, fetch_ohlcv, calculate_indicators, check_signals, itp. pozostają bez zmian
# Zakładam, że są zdefiniowane w Twoim kodzie. Jeśli nie, proszę podaj je, a dodam.

async def main():
    positions = load_positions()
    positions = sync_with_wallet(positions)
    since = int((datetime.now() - timedelta(days=data_days)).timestamp() * 1000)

    logging.info("Bot uruchomiony.")
    print(f"{Fore.BLUE}Bot uruchomiony. Handel na top 70 parach z kapitałem 60 USDT.{Style.RESET_ALL}")

    last_pair_update = 0
    last_price_update = 0
    symbols = []
    exchange_counter = 0

    while True:
        try:
            current_time = time.time()

            if current_time - last_pair_update > 3600:
                symbols = get_top_pairs(exchange_counter)
                exchange_counter += 1
                if not symbols:
                    logging.error("Nie udało się pobrać listy par. Czekam 60 sekund.")
                    await asyncio.sleep(60)
                    continue
                last_pair_update = current_time

            if current_time - last_price_update > price_update_interval:
                await update_position_prices(positions, exchange_counter)
                exchange_counter += 1
                last_price_update = current_time

            available_capital = get_balance(exchange_counter)
            exchange_counter += 1
            buy_offers = []

            for i, key in enumerate(list(positions.keys())):
                try:
                    symbol = key.split('_', 1)[0] if '_' in key else key
                    strategy_name = positions[key]['strategy']['name']
                    pos = positions[key]
                    current_price = pos['current_price']
                    sl_price = pos['sl_price']
                    tp_price = pos['tp_price']
                    amount = pos['amount']

                    if 'tp_order_id' not in pos or 'sl_order_id' not in pos:
                        tp_order = await place_limit_order(symbol, 'sell', amount, tp_price, i)
                        sl_order = await place_limit_order(symbol, 'sell', amount, sl_price, i)
                        if tp_order and sl_order:
                            pos['tp_order_id'] = tp_order['id']
                            pos['sl_order_id'] = sl_order['id']
                            save_positions(positions)
                        else:
                            logging.error(f"Nie udało się wystawić zleceń TP/SL dla {key}")
                            continue

                    exchange = exchanges[i % len(exchanges)]
                    for order_id, reason in [(pos['tp_order_id'], 'TP'), (pos['sl_order_id'], 'SL')]:
                        try:
                            order = exchange.fetch_order(order_id, symbol)
                            if order['status'] == 'closed':
                                profit = (order['price'] - pos['entry_price']) * amount * (1 - fee_rate)
                                closed_position = {
                                    'symbol': symbol,
                                    'strategy': strategy_name,
                                    'profit': profit,
                                    'close_time': datetime.now().isoformat(),
                                    'reason': reason
                                }
                                logging.info(f"Zamknięto pozycję ({reason}) dla {key}: zysk/strata={profit:.2f} USDT")
                                save_closed_positions(closed_position)
                                other_order_id = pos['sl_order_id'] if reason == 'TP' else pos['tp_order_id']
                                await cancel_order(symbol, other_order_id, i)
                                del positions[key]
                                save_positions(positions)
                        except Exception as e:
                            logging.error(f"Błąd przy sprawdzaniu zlecenia {order_id} dla {key} ({exchange.exchange_id}): {e}")
                except Exception as e:
                    logging.error(f"Błąd przy sprawdzaniu SL/TP dla {key} ({exchanges[i % len(exchanges)].exchange_id}): {e}")

            if (current_time % scan_interval) < sl_tp_interval:
                for symbol in symbols:
                    for strategy in strategies:
                        position_key = f"{symbol}_{strategy['name']}"
                        if position_key in positions:
                            continue
                        try:
                            df = fetch_ohlcv(symbol, timeframe, since, exchange_counter)
                            exchange_counter += 1
                            if df is None or df.empty:
                                continue
                            df_indicators = calculate_indicators(df.copy(), strategy['params'])
                            if df_indicators is None:
                                continue
                            buy_signal = check_signals(df_indicators, strategy)
                            if buy_signal and len(positions) < max_positions and available_capital >= capital_per_position:
                                buy_offers.append({'symbol': symbol, 'strategy': strategy['name']})
                                try:
                                    ticker = exchanges[exchange_counter % len(exchanges)].fetch_ticker(symbol)
                                    current_price = ticker['last']
                                    amount = capital_per_position / current_price
                                    order = place_order(symbol, 'buy', amount, current_price, exchange_counter)
                                    exchange_counter += 1
                                    if order:
                                        sl_price = current_price * (1 - strategy['params']['sl_percent'] / 100)
                                        tp_price = current_price * (1 + strategy['params']['tp_percent'] / 100)
                                        positions[position_key] = {
                                            'amount': amount,
                                            'entry_price': current_price,
                                            'strategy': strategy,
                                            'buy_time': datetime.now(),
                                            'current_price': current_price,
                                            'tp_price': tp_price,
                                            'sl_price': sl_price
                                        }
                                        logging.info(f"Otwarto pozycję dla {position_key}: ilość={amount}, cena={current_price}")
                                        save_positions(positions)
                                        break
                                except Exception as e:
                                    logging.error(f"Błąd przy kupnie {symbol} ({exchanges[exchange_counter % len(exchanges)].exchange_id}): {e}")
                        except Exception as e:
                            logging.error(f"Błąd przy skanowaniu {symbol} ({exchanges[exchange_counter % len(exchanges)].exchange_id}): {e}")

            display_interface(positions, buy_offers, available_capital, symbols)
            await asyncio.sleep(sl_tp_interval)
        except Exception as e:
            logging.error(f"Krytyczny błąd w pętli głównej: {e}")
            await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot zatrzymany przez użytkownika.")
        print(f"{Fore.RED}Bot zatrzymany.{Style.RESET_ALL}")
    except Exception as e:
        logging.error(f"Krytyczny błąd: {e}")
        print(f"{Fore.RED}Krytyczny błąd: {e}{Style.RESET_ALL}")
