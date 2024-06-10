import sqlalchemy
import schedule
from time import sleep
from redis import Redis
from datetime import datetime
# Custom Modules
from spellbook.dataGather import balance_info, trans_history, time_files_open, trade_info
from spellbook.tradeFunc import open_trade_GTX, close_trade, open_trail, slider_convert, balance_changer, btc_to_stable
from spellbook.helpers import candles_format, gain_cal, send_push
from bestiary.accounts import ACCOUNTS
from spellbook.redis_names import get_cache, get_control, get_indicator
import spellbook.zkr_binance as zkr_binance


class Bot:
    """The main brains behind the operation. This class is the bot itself. It contains all the functions for
    running the bot. All the bots are instances of this class."""

    def __init__(self, acc_id: str = "TEST", pair: str = "BTCUSDT") -> None:
        """Initialize Bot
        :param acc_id: Account ID as defined in accounts.py
        :param pair: Trading pair
        """
        # ---CONSTANTS---
        self.ACC = ACCOUNTS[acc_id]
        self.LIMIT = self.ACC["BOT_INFO"]["LIMIT"]  # limit for the candle look back
        self.LONG = self.ACC["BOT_INFO"]["LONG%"]  # Long %
        self.SHORT = self.ACC["BOT_INFO"]["SHORT%"]  # Short %
        self.GATE = self.ACC["BOT_INFO"]["EMA_LIMIT"]  # [HIGH, LOW] GATE 1
        self.ACC_ID = self.ACC["BOT_INFO"]["ID"]  # Bot ID
        self.PAIR = pair  # Trading Pair
        self.AMMO = self.ACC["BOT_INFO"][f"AMMO_{self.PAIR[3:]}"]  # Bullets for trading
        self.INTERVAL_1 = self.ACC["BOT_INFO"]["INTERVAL"]  # Interval for the candles
        self.REDIS_CACHE = get_cache(self.ACC_ID, self.PAIR)  # Redis Cache names
        self.REDIS_CONTROL = get_control(self.ACC_ID, self.PAIR)  # Redis Control names
        self.REDIS_INDICATOR = get_indicator(self.ACC_ID, self.PAIR)  # Redis Indicator names
        # ---VARIABLES---
        self.schedule = schedule  # Scheduler for timed events
        self.balance_ratio = None  # Balance Ratio Initialization
        self.entry_price = None  # Entry Price Initialization
        self.exit_price = None  # Exit Price Initialization
        self.trail_price_sell = None  # Trail Price Sell Initialization
        self.trail_price_buy = None  # Trail Price Buy Initialization
        self.positions = None  # Position Initialization
        self.account = None  # Account Initialization
        self.candles = None  # Candles Initialization
        self.price = None  # Price Initialization
        self.in_trade = False  # In Trade Check
        self.can_trade = False  # Can Trade Check based on 1 hour RSI
        self.btc, self.usdt, self.busd = 0, 0, 0  # Balance Initialization
        self.errors = 0  # Error Counter, used in the children of the Bot
        self.long_amount = 0  # Long based on LONG%
        self.short_amount = 0  # Short based on SHORT%
        self.acc_equity = 0.0  # Account Equity
        self.pnl = 0.0  # PNL Initialization
        self.roe = 0.0  # Return on Equity Initialization
        self.low_price = 0.0  # Low Price Initialization for Trailing Stop
        self.now = datetime.now()  # Current Time
        self.now_pnl = datetime.now()  # Current Time for PNL
        # ---API AND KEY ASSIGNMENT---
        self.PUSHAPI = self.ACC["APIS"]["PUSHBULLET"]  # Pushbullet API
        self.APIKEY_TRADE = self.ACC["APIS"]["API_TRADE"]  # Trader
        self.SECRET_TRADE = self.ACC["APIS"]["SEC_TRADE"]
        self.APIKEY_DATA = self.ACC["APIS"]["API_DATA"]  # Account Reader
        self.SECRET_DATA = self.ACC["APIS"]["SEC_DATA"]
        self.REDIS_PW = self.ACC["APIS"]["REDIS_PW"]  # Redis Password
        self.REDIS_HOST = self.ACC["APIS"]["REDIS_HOST"]  # Redis Host
        self.engine = sqlalchemy.create_engine(self.ACC["APIS"]["ENGINE"])  # SQL Engine
        self.r = Redis(host=self.REDIS_HOST, port=6379, db=0, password=self.REDIS_PW)  # Redis Connection
        self._redis_ini()  # Redis Initialization
        self.r.set(self.REDIS_CACHE["alive"], 1)  # Set the bot to alive
        self.get_stuff()  # Get the initial data
        self.schedule.every(1).hours.do(self._data_to_sql)  # Schedule data to SQL
        self.schedule.every(5).minutes.do(self._pnl_to_sql)  # Schedule PNL to SQL
        self.schedule.every(1).hours.do(self._redis_ini)  # Schedule Redis Initialization

    def get_stuff(self) -> None:
        """This function runs in every strategy.
        It combines all the private GET functions from the class.
        :param self: Bot Class
        :return: None"""
        self._balance_get()  # Get the account Balance
        self._change_balance()  # Change the balance ratio if the slider is on
        self._trade_get()  # Get the trading info of the account
        self._klines_get()  # Get the candles for technical analysis and price
        self._in_trade_get()  # Get the PNL and ROE if in trade
        self.schedule.run_pending()  # Run the scheduled tasks

    def trail_enter(self, signal_1: int, signal_2: int, side: str) -> None:
        """Enter a trailing stop trade, remember time of entry and add to SQL"""
        if side == "SELL":
            self.r.set(self.REDIS_CACHE["trail_sell"], self.price)
            self.r.set(self.REDIS_CACHE["exit_price"], self.price)
        elif side == "BUY":
            self.r.set(self.REDIS_CACHE["trail_buy"], self.price)
            self.r.set(self.REDIS_CACHE["open_price"], self.price)

        # Send it to SQL
        df = open_trail(self.price,
                        self.long_amount, self.short_amount,
                        signal_1, signal_2,
                        self.in_trade, self.pnl, self.PAIR, side)

        df.to_sql(name=f"trades_{self.ACC_ID}", con=self.engine, index=False, if_exists="append")

    def enter_trade(self, signal_1: int, signal_2: int) -> None:
        """Function that enters the trade. It is called from the strategy.
        :param self: Bot Class
        :param signal_1: First signal for entry
        :param signal_2: Second signal for entry
        :return: None"""
        bullets = (round(self.btc, 4) * self.AMMO)  # Amount of capital available for trade
        if bullets < 0.001:  # if available for trade BTC is less than 0.001, do not trade
            self.in_trade = False
        else:
            self.in_trade = True  # Set in trade to True
            # --------Calc the amount to be bought--------
            self.long_amount = round(bullets * self.LONG, 3)
            self.short_amount = round(bullets * self.SHORT, 3)
            # --------------Open the trade----------------
            df = open_trade_GTX(self.APIKEY_TRADE, self.SECRET_TRADE, self.PAIR,
                                self.long_amount, self.short_amount,
                                signal_1, signal_2, self.in_trade)
            df.to_sql(name=f"trades_{self.ACC_ID}", con=self.engine, index=False, if_exists="append")
            # ----------Cache the open time and price----------
            self.r.set(self.REDIS_CACHE["open_time"], int(df["timestamp"][0]))
            self.r.set(self.REDIS_CACHE["open_price"], float(self.price))
            self.r.set(self.REDIS_CACHE["trail_buy"], 0)
            # ----------Update the balance table----------
            bi = balance_info(self.account, self.PAIR, self.price, self.ACC_ID)
            bi.to_sql(name=f"balanceInfo_{self.ACC_ID}", con=self.engine, index=False, if_exists="append")

    def exit_trade(self, signal_1: int, signal_2: int, man_close: bool = False) -> None:
        """Close the trade, convert the gain to BTC.
        :param self: Bot Class
        :param signal_1: First signal for exit
        :param signal_2: Second signal for exit
        :param man_close: For manual close
        :return: None"""
        # Reset the statistics
        self.in_trade = False  # Set in trade to False
        self.pnl = 0.0  # Reset the PNL
        self.roe = 0.0  # Reset the ROE
        # --------------Close the trade----------------
        df = close_trade(self.APIKEY_TRADE, self.SECRET_TRADE, self.PAIR,
                         self.long_amount, self.short_amount,
                         signal_1, signal_2, self.in_trade, self.entry_price,
                         man_close=man_close)
        df.to_sql(name=f"trades_{self.ACC_ID}", con=self.engine, index=False, if_exists="append")
        # -------------Reset Trade Cache---------------
        self.r.set(self.REDIS_CACHE["trail_sell"], 0)
        self.r.set(self.REDIS_CACHE["exit_price"], 0)
        self.r.set(self.REDIS_CACHE["open_time"], 0)
        self.r.set(self.REDIS_CACHE["open_price"], 0)
        self.r.set(self.REDIS_INDICATOR["short_pnl"], 0)
        self.r.set(self.REDIS_INDICATOR["long_pnl"], 0)
        sleep(5)
        # ------------Update the transaction table------------
        # Get the transaction history from Binance
        hist = zkr_binance.get_income_history_api(self.APIKEY_TRADE, self.SECRET_TRADE,
                                                  start_time=time_files_open(self.ACC_ID), limit=1000)
        # Split the transaction history into transactions and transfers
        th, tfh = trans_history(hist, self.ACC_ID)
        if not th.empty:  # If there is a new transaction, send it to SQL
            th.to_sql(name=f"transHist_{self.ACC_ID}_new",
                      con=self.engine, if_exists='append', index=False)
        if not tfh.empty:  # If there is a new transfer, send it to SQL
            tfh.to_sql(name=f"transferHist_{self.ACC_ID}",
                       con=self.engine, if_exists='append', index=False)
        # --------------PNL To BTC----------------
        if float(df['pnl'][0]) > 0:  # If PNL is positive, convert to BTC
            slider_convert(pnl=(df['pnl'][0]),
                           API=self.APIKEY_TRADE,
                           SECRET=self.SECRET_TRADE,
                           acc_id=self.ACC_ID,
                           pair=self.PAIR,
                           r=self.r,
                           price=self.price)
        # If PNL is negative, convert to stable coin
        if float(df['pnl'][0]) < (-10):  # If PNL is negative, convert to stable coin
            print("Negative PNL, converting to stable coin")
            btc_to_stable(API=self.APIKEY_TRADE,
                          SECRET=self.SECRET_TRADE,
                          amount=float(df['pnl'][0]) * 1.1,  # 10% more
                          pair=self.PAIR,
                          price=self.price)

    def bot_stop(self) -> None:
        """Stop the bot. Set the alive key to 0. Send a push notification.
        :return: None"""
        sleep(1)
        self.r.set(self.REDIS_CACHE["alive"], 0)
        send_push(title=f"Bot Stopped {self.ACC_ID} | {self.PAIR}",
                  body="Bot Stopped",
                  access_token=self.PUSHAPI)

    def get_bot_status(self) -> int:
        """Check if the bot is alive. Used to keep the loop running. Stored in Redis.
        :return: 1 if alive, 0 if not"""
        return int(self.r.get(self.REDIS_CACHE["alive"]))

    def _trade_get(self) -> None:
        """Check if the account is in a trade. If so, get the trade info. Pulls data from Binance and Redis.
        :return: None"""
        # Get Account position info from binance
        self.positions = zkr_binance.get_position_api(api_key=self.APIKEY_TRADE,
                                                      api_secret=self.SECRET_TRADE,
                                                      symbol=self.PAIR)
        if float(self.positions[0]['positionAmt']) > 0:  # If  long position more than 0, in trade
            self.long_amount = float(self.positions[0]['positionAmt'])  # Get the long amount
            self.short_amount = abs(float(self.positions[1]['positionAmt']))  # Get the short amount
            self.entry_price = float(self.positions[1]['entryPrice'])  # Get the entry price
            self.trail_price_sell = float(self.r.get(self.REDIS_CACHE["trail_sell"]))  # Get the trail price
            self.exit_price = float(self.r.get(self.REDIS_CACHE["exit_price"]))  # Get the exit price
            self.in_trade = True  # Set in trade to True
        else:  # not in trade or trailing open
            self.trail_price_buy = float(self.r.get(self.REDIS_CACHE["trail_buy"]))  # Get the trail price
            self.in_trade = False  # Set in trade to False

    def _klines_get(self) -> None:
        """Get the candles and price from Binance API. Sets the price and candles variables.
        :return: None"""
        candle = zkr_binance.get_klines(symbol=self.PAIR,
                                        interval=self.INTERVAL_1,
                                        limit=self.LIMIT)  # Get the candles of the given interval
        self.price = float(candle[self.LIMIT - 1][4])  # Get the current price from the candle
        self.candles = candles_format(candle)  # Format the candles for technical analysis

    def _balance_get(self) -> None:
        """Get balance of the Binance account: BTC, USDT, BUSD and the balance ratio.
        :return: None"""
        # Get Account info from binance
        self.account = zkr_binance.get_account_api(self.APIKEY_DATA, self.SECRET_DATA)

        assets = self.account["assets"]  # Get assets
        for i in range(len(assets)):
            if assets[i]['asset'] == 'BTC':  # Get BTC balance
                self.btc = round(float(assets[i]['walletBalance']), 5)
            elif assets[i]['asset'] == 'USDT':  # Get USDT balance
                self.usdt = round(float(assets[i]['walletBalance']), 2)
            elif assets[i]['asset'] == 'USDC':  # Get USDC balance
                self.busd = round(float(assets[i]['walletBalance']), 2)

        # Used for balance slider
        self.acc_equity = round(float(self.account['totalMarginBalance']), 2)  # Get the account equity
        self.balance_ratio = round((self.usdt + self.busd) / self.acc_equity, 3)  # Get the balance ratio
        self.r.set(self.REDIS_INDICATOR["act_balance"], self.balance_ratio)  # Set the balance ratio to Redis

    def _in_trade_get(self) -> None:
        """When in trade calculate PNL with Fees and save it to Redis.
        :return: None"""
        if self.in_trade:
            short_gain = float(self.positions[1]['unRealizedProfit'])  # Get the short PNL
            long_gain = float(self.positions[0]['unRealizedProfit'])  # Get the long PNL
            self.r.set(self.REDIS_INDICATOR["short_pnl"], short_gain)  # Set the short PNL to Redis
            self.r.set(self.REDIS_INDICATOR["long_pnl"], long_gain)  # Set the long PNL to Redis
            # Calculate PNL and ROE with 2 fees
            self.pnl = gain_cal(short_gain, long_gain, self.short_amount, self.long_amount, self.price)
            print(f"Current PNL: {self.pnl}")
            self.roe = round((self.pnl / self.acc_equity * 100), 3)
            print(f"ROE: {self.roe}%")

    def _change_balance(self) -> None:
        """Change the balance ratio if the balance slider is on.
        Uses Redis as trigger as well as the balance slider value.
        :return: None"""
        # If the balance slider is detected on. Change the balance ratio
        if int(self.r.get(self.REDIS_CONTROL["balance_trigger"])) == 1:
            print("Balance slider is on")
            self.r.set(self.REDIS_CONTROL["balance_trigger"], 0)  # Reset the balance trigger
            new_ratio = float(self.r.get(self.REDIS_CONTROL["balance_slider"]))  # Get the new balance ratio
            if new_ratio < 0.1:  # Limit it to 10%
                new_ratio = 0.1
            # Change the balance ratio
            balance_changer(account=self.account,
                            new_ratio=new_ratio,
                            price=self.price,
                            API=self.APIKEY_TRADE,
                            SECRET=self.SECRET_TRADE)

    def _data_to_sql(self) -> None:
        """Send the account information to SQL. Balance and transaction history.
        The transaction history is only sent if there is a new transaction.
        :param self:
        :return: None"""
        # Balance info to SQL
        bi = balance_info(self.account, self.PAIR, self.price, self.ACC_ID)  # Get the balance info from Binance
        bi.to_sql(name=f"balanceInfo_{self.ACC_ID}", con=self.engine, index=False, if_exists="append")  # Send it to SQL

        hist = zkr_binance.get_income_history_api(self.APIKEY_TRADE, self.SECRET_TRADE,  # Get the transaction history
                                                  start_time=time_files_open(self.ACC_ID), limit=1000)
        th, tfh = trans_history(hist, self.ACC_ID)  # Split the transaction history into transactions and transfers
        if not th.empty:  # If there is a new transaction, send it to SQL
            th.to_sql(f"transHist_{self.ACC_ID}_new", self.engine, if_exists='append', index=False)
        if not tfh.empty:  # If there is a new transfer, send it to SQL
            tfh.to_sql(f"transferHist_{self.ACC_ID}", self.engine, if_exists='append', index=False)

    def _pnl_to_sql(self) -> None:
        """Send the PNL to SQL.
        :param self:
        :return: None"""
        if self.in_trade:  # If in trade, send the PNL Data to SQL
            ti = trade_info(self.positions, self.PAIR, self.LONG, self.SHORT,
                            self.pnl, 0, self.ACC_ID)
            ti.to_sql(name=f"pnlInfo_{self.ACC_ID}_{self.PAIR.lower()}", con=self.engine, index=False,
                      if_exists="append")

    def _redis_ini(self) -> None:
        """Initialize Redis DB.
        If the key does not exist, set it to 0.
        :return: None"""
        for key in self.REDIS_CACHE.values():
            if self.r.get(key) is None:
                self.r.set(key, 0)
        for key in self.REDIS_CONTROL.values():
            if self.r.get(key) is None:
                self.r.set(key, 0)
        for key in self.REDIS_INDICATOR.values():
            if self.r.get(key) is None:
                self.r.set(key, 0)

    def _redis_bu(self) -> None:
        """Backup Redis DB."""
        self.r.bgsave()

    def redis_reconnect(self) -> None:
        """Reconnect Redis DB."""
        self.r = Redis(host=self.REDIS_HOST, port=6379, db=0, password=self.REDIS_PW)
