import websocket
import threading
import traceback
import decimal
import ssl
from time import sleep
import json
import logging
import urllib
import math
from market_maker.utils.api_key import generate_nonce, generate_signature
from market_maker.settings import settings
from future.utils import iteritems
from future.standard_library import hooks
with hooks():  # Python 2/3 compat
    from urllib.parse import urlparse, urlunparse
# Naive implementation of connecting to BitMEX websocket for streaming realtime data.
# The Marketmaker still interacts with this as if it were a REST Endpoint, but now it can get
# much more realtime data without polling the hell out of the API.
#
# The Websocket offers a bunch of data as raw properties right on the object.
# On connect, it synchronously asks for a push of all this data then returns.
# Right after, the MM can start using its data. It will be updated in realtime, so the MM can
# poll really often if it wants.
class BitMEXWebsocket:

    # Don't grow a table larger than this amount. Helps cap memory usage.
    MAX_TABLE_LEN = 200

    def __init__(self, endpoint, symbol, api_key=None, api_secret=None):
        self.logger = logging.getLogger('root')
        self.__reset()
        # self.logger.debug("Initializing WebSocket.")

        # self.endpoint = endpoint
        # self.symbol = symbol

        # if api_key is not None and api_secret is None:
        #     raise ValueError('api_secret is required if api_key is provided')
        # if api_key is None and api_secret is not None:
        #     raise ValueError('api_key is required if api_secret is provided')

        # self.api_key = api_key
        # self.api_secret = api_secret

        # self.data = {}
        # self.keys = {}
        # self.exited = False

        # # We can subscribe right in the connection querystring, so let's build that.
        # # Subscribe to all pertinent endpoints
        # wsURL = self.__get_url()
        # self.logger.info("Connecting to %s" % wsURL)
        # self.__connect(wsURL, symbol)
        # self.logger.info('Connected to WS.')

        # # Connected. Wait for partials
        # self.__wait_for_symbol(symbol)
        # if api_key:
        #     self.__wait_for_account()
        # self.logger.info('Got all market data. Starting.')

    def connect(self, endpoint="", symbol="XBTN15", shouldAuth=True):
        self.logger.debug("Connecting WebSocket.")
        self.symbol = symbol
        self.shouldAuth = shouldAuth

        # We can subscribe right in the connection querystring, so let's build that.
        # Subscribe to all pertinent endpoints
        subscriptions = [sub + ':' + symbol for sub in ["quote", "trade", "orderBookL2"]]
        subscriptions += ["instrument"] # We want all of them
        if self.shouldAuth:
            subscriptions += [sub + ':' + symbol for sub in ["order", "execution"]]
            subscriptions += ["margin", "position"]

        # Get WS URL and connect.
        urlParts = list(urlparse(endpoint))
        urlParts[0] = urlParts[0].replace('http', 'ws')
        urlParts[2] = "/realtime?subscribe=" + ",".join(subscriptions)
        wsURL = urlunparse(urlParts)
        self.logger.info("Connecting to %s" % wsURL)
        self.__connect(wsURL, symbol)
        self.logger.info('Connected to WS. Waiting for data images, this may take a moment...')

        # Connected. Wait for partials
        self.__wait_for_symbol(symbol)
        if self.shouldAuth:
            self.__wait_for_account()
        self.logger.info('Got all market data. Starting.')

    def exit(self):
        '''Call this to exit - will close websocket.'''
        self.exited = True
        self.ws.close()

    def get_instrument(self):
        '''Get the raw instrument data for this symbol.'''
        # Turn the 'tickSize' into 'tickLog' for use in rounding
        instrument = self.data['instrument'][0]
        instrument['tickLog'] = decimal.Decimal(str(instrument['tickSize'])).as_tuple().exponent * -1
        return instrument

    def get_ticker(self):
        '''Return a ticker object. Generated from quote and trade.'''
        max_buy = 0
        min_sell = 100000
        for o in self.data['orderBookL2']:
            if o['side'] == 'Buy':
                if o['price'] > max_buy:
                    max_buy = o['price']
            if o['side'] == 'Sell':
                if o['price'] < min_sell:
                    min_sell = o['price']

        if max_buy > min_sell:
            max_buy = self.data['trade'][0]['price'] - 0.5
            min_sell = self.data['trade'][0]['price'] + 0.5
        print("last trade : %f" % self.data['trade'][0]['price'])

        #print(self.data['orderBookL2'][len(self.data['orderBookL2']) / 2])
        ticker = {
            "last": round((max_buy * 2)) / 2,
            "buy": round((max_buy * 2)) / 2,
            "sell": round((min_sell * 2)) / 2,
            "mid": round((min_sell * 2)) / 2
        }

        # The instrument has a tickSize. Use it to round values.
        #instrument = self.data['instrument'][0]
        return ticker

    def funds(self):
        '''Get your margin details.'''
        return self.data['margin'][0]

    def market_depth(self, symbol):
        #raise NotImplementedError('orderBook is not subscribed; use askPrice and bidPrice on instrument')
        sum_buy = 0
        sum_sell = 0
        ticker = self.get_ticker()
        #instrument = self.get_instrument()
        if self.data['trade'] is None:
            return 0
        for order in self.data['trade']:
            if order["side"] == "Buy" and (order['price'] - (ticker['buy'])) > -1 :
                sum_buy += order["size"]
            elif order['side'] == "Sell" and (order['price'] - ticker['sell']) < 1:
                sum_sell += order["size"]
        self.logger.info("sum buy : %f", sum_buy)
        self.logger.info("sum_sell : %f", sum_sell)
        if (sum_buy != 0 and sum_sell != 0):
            ask_thresold = float(sum_buy - sum_sell) / float(sum_buy + sum_sell)
            bid_thresold = float(sum_sell - sum_buy) / float(sum_buy + sum_sell)
        else:
            ask_thresold = 0

        #self.logger.info("ask_thresold : %f", ask_thresold)
        #self.logger.info("bid_thresold : %f", bid_thresold)
        return ask_thresold
        #return self.data['orderBookL2']
    def open_orders(self, clOrdIDPrefix):
        '''Get all your open orders.'''
        orders = self.data['order']
        # Filter to only open orders (leavesQty > 0) and those that we actually placed
        return [o for o in orders if str(o['clOrdID']).startswith(clOrdIDPrefix) and o['leavesQty'] > 0]

    def position(self, symbol):
        positions = self.data['position']
        pos = [p for p in positions if p['symbol'] == symbol]
        if len(pos) == 0:
            # No position found; stub it
            return {'avgCostPrice': 0, 'avgEntryPrice': 0, 'currentQty': 0, 'symbol': symbol}
        return pos[0]
    def recent_trades(self):
        '''Get recent trades.'''
        return self.data['trade']

    #
    # End Public Methods
    #

    def __connect(self, wsURL, symbol):
        '''Connect to the websocket in a thread.'''
        self.logger.debug("Starting thread")

        self.ws = websocket.WebSocketApp(wsURL,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error,
                                         header=self.__get_auth())

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        self.wst.start()
        self.logger.debug("Started thread")

        # Wait for connect before continuing
        conn_timeout = 100
        while not self.ws.sock or not self.ws.sock.connected and conn_timeout:
            sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            self.logger.info("Couldn't connect to WS! Exiting.")
            #self.exit()
            #raise websocket.WebSocketTimeoutException('Couldn\'t connect to WS! Exiting.')

    def __get_auth(self):
        '''Return auth headers. Will use API Keys if present in settings.'''
        if self.shouldAuth:
            self.logger.info("Authenticating with API Key.")
            # To auth to the WS using an API key, we generate a signature of a nonce and
            # the WS API endpoint.
            nonce = generate_nonce()
            return [
                "api-nonce: " + str(nonce),
                "api-signature: " + generate_signature(settings.API_SECRET, 'GET', '/realtime', nonce, ''),
                "api-key:" + settings.API_KEY
            ]
        else:
            self.logger.info("Not authenticating.")
            return []

    def __get_url(self):
        '''
        Generate a connection URL. We can define subscriptions right in the querystring.
        Most subscription topics are scoped by the symbol we're listening to.
        '''

        # You can sub to orderBookL2 for all levels, or orderBook10 for top 10 levels & save bandwidth
        symbolSubs = ["execution", "instrument", "order", "orderBookL2", "position", "quote", "trade"]
        genericSubs = ["margin"]

        subscriptions = [sub + ':' + self.symbol for sub in symbolSubs]
        subscriptions += genericSubs

        urlParts = list(urllib.parse.urlparse(self.endpoint))
        urlParts[0] = urlParts[0].replace('http', 'ws')
        urlParts[2] = "/realtime?subscribe={}".format(','.join(subscriptions))
        return urllib.parse.urlunparse(urlParts)

    def __wait_for_account(self):
        '''On subscribe, this data will come down. Wait for it.'''
        # Wait for the keys to show up from the ws
        while not {'margin', 'position', 'order'} <= set(self.data):
            sleep(0.1)

    def __wait_for_symbol(self, symbol):
        '''On subscribe, this data will come down. Wait for it.'''
        while not {'instrument', 'trade', 'quote'} <= set(self.data):
            sleep(0.1)

    def __send_command(self, command, args=None):
        '''Send a raw command.'''
        if args is None:
            args = []
        self.ws.send(json.dumps({"op": command, "args": args}))

    def __on_message(self, ws, message):
        '''Handler for parsing WS messages.'''
        message = json.loads(message)
        self.logger.debug(json.dumps(message))

        table = message['table'] if 'table' in message else None
        action = message['action'] if 'action' in message else None
        try:
            if 'subscribe' in message:
                self.logger.debug("Subscribed to %s." % message['subscribe'])
            elif action:

                if table not in self.data:
                    self.data[table] = []

                if table not in self.keys:
                    self.keys[table] = []
                # There are four possible actions from the WS:
                # 'partial' - full table image
                # 'insert'  - new row
                # 'update'  - update row
                # 'delete'  - delete row
                if action == 'partial':
                    self.logger.debug("%s: partial" % table)
                    self.data[table] += message['data']
                    # Keys are communicated on partials to let you know how to uniquely identify
                    # an item. We use it for updates.
                    self.keys[table] = message['keys']
                elif action == 'insert':
                    self.logger.debug('%s: inserting %s' % (table, message['data']))
                    self.data[table] += message['data']

                    # Limit the max length of the table to avoid excessive memory usage.
                    # Don't trim orders because we'll lose valuable state if we do.
                    if table not in ['order', 'orderBookL2'] and len(self.data[table]) > BitMEXWebsocket.MAX_TABLE_LEN:
                        self.data[table] = self.data[table][int(BitMEXWebsocket.MAX_TABLE_LEN / 2):]

                elif action == 'update':
                    self.logger.debug('%s: updating %s' % (table, message['data']))
                    # Locate the item in the collection and update it.
                    for updateData in message['data']:
                        item = findItemByKeys(self.keys[table], self.data[table], updateData)
                        if not item:
                            return  # No item found to update. Could happen before push
                        item.update(updateData)
                        # Remove cancelled / filled orders
                        if table == 'order' and item['leavesQty'] <= 0:
                            self.data[table].remove(item)
                elif action == 'delete':
                    self.logger.debug('%s: deleting %s' % (table, message['data']))
                    # Locate the item in the collection and remove it.
                    for deleteData in message['data']:
                        item = findItemByKeys(self.keys[table], self.data[table], deleteData)
                        if item in self.data[table]:
                            self.data[table].remove(item)
                else:
                    raise Exception("Unknown action: %s" % action)
        except:
            self.logger.info(traceback.format_exc())
            #self.exit()

    def error(self, err):
        self._error = err
        self.logger.error(err)
        #self.exit()
    def __on_error(self, ws, error):
        '''Called on fatal websocket errors. We exit on these.'''
        if not self.exited:
            self.logger.error("Error : %s" % error)
            self.error(error)

    def __on_open(self, ws):
        '''Called when the WS opens.'''
        self.logger.debug("Websocket Opened.")

    def __on_close(self, ws):
        '''Called on websocket close.'''
        self.logger.info('Websocket Closed')
        self.exit()

    def __reset(self):
        self.data = {}
        self.keys = {}
        self.exited = False
        self._error = None


# Utility method for finding an item in the store.
# When an update comes through on the websocket, we need to figure out which item in the array it is
# in order to match that item.
#
# Helpfully, on a data push (or on an HTTP hit to /api/v1/schema), we have a "keys" array. These are the
# fields we can use to uniquely identify an item. Sometimes there is more than one, so we iterate through all
# provided keys.
def findItemByKeys(keys, table, matchData):
    for item in table:
        matched = True
        for key in keys:
            if item[key] != matchData[key]:
                matched = False
        if matched:
            return item
