import logging
import time
from datetime import datetime, timedelta # Added timedelta, often useful with time constants
import json
import os # For token file path checking in Worker's token methods
from collections import deque # Used in Worker for various queues
from threading import Thread, Lock # For Worker's background tasks and resource protection
from typing import Optional, Dict, Any, List, Union # Union added for flexibility
from dataclasses import dataclass, field # For OrderData

from PyQt6.QtCore import QObject, pyqtSignal, QTimer # QObject for signals, QTimer for periodic tasks in Worker

# Assuming schwab.client.Client is the correct path
# This aligns with previous steps.
from schwab.client import Client

# --- Constants for Trading Logic ---
API_RATE_LIMIT_PER_MINUTE = 120
TOKEN_REFRESH_BUFFER_SECONDS = 300  # 5 minutes
TOKEN_REFRESH_RETRY_DELAY_MS = 30000  # 30 seconds
ORDER_MONITOR_INTERVAL_MS = 5000  # 5 seconds
TOKEN_REFRESH_CHECK_INTERVAL_MS = 60000 # 1 minute
ORDER_ACTIVE_ORDERS_MAX = 50 # Max active orders to track or query

SERVICE_LEVELONE_EQUITIES = "LEVELONE_EQUITIES"
SERVICE_ACCT_ACTIVITY = "ACCT_ACTIVITY" # Account activity service name

ORDER_STATUS_FILLED = "FILLED"
ORDER_STATUS_CANCELED = "CANCELED" # Note: Prompt had CANCELLED, Schwab API often uses CANCELED
ORDER_STATUS_REJECTED = "REJECTED"
ORDER_STATUS_WORKING = "WORKING" # Added, common status
ORDER_STATUS_OPEN = "OPEN" # Added, common status
ORDER_STATUS_PENDING_ACTIVATION = "PENDING_ACTIVATION" # Common for complex orders
ORDER_STATUS_EXPIRED = "EXPIRED" # Common status

INSTR_BUY = "BUY"
INSTR_SELL = "SELL"
ASSET_EQUITY = "EQUITY"


# --- Validator Class ---
# Moved here in previous subtask, ensure it's complete.
class Validator:
    @staticmethod
    def is_valid_symbol(symbol: str) -> bool:
        return isinstance(symbol, str) and 0 < len(symbol) <= 5 and symbol.isalpha() and symbol.isupper()

    @staticmethod
    def is_valid_price(price_str: Union[str, float, int]) -> bool:
        try:
            price = float(price_str)
            return price > 0
        except ValueError:
            return False

    @staticmethod
    def is_valid_quantity(qty_str: Union[str, int]) -> bool:
        try:
            qty = int(qty_str)
            return qty > 0
        except ValueError:
            return False

    @staticmethod
    def is_valid_time(time_str: str) -> bool:
        try:
            datetime.strptime(time_str, "%H:%M:%S")
            return True
        except ValueError:
            return False

# --- OrderData Dataclass ---
@dataclass
class OrderData:
    order_id: str
    client_order_id: str # Custom ID used when placing the order
    symbol: str
    quantity: int
    filled_quantity: int = 0
    price: Optional[float] = None # Limit price
    avg_fill_price: Optional[float] = None
    status: str = "UNKNOWN" # e.g., WORKING, FILLED, CANCELED
    instruction: str = "" # BUY, SELL
    asset_type: str = ASSET_EQUITY
    timestamp: float = field(default_factory=time.time)
    raw_response: Optional[Dict[str, Any]] = None # Store the raw order response if needed

    def __str__(self):
        return (f"Order(id={self.order_id}, sym={self.symbol}, qty={self.quantity}, "
                f"filled={self.filled_quantity}, status={self.status}, instr={self.instruction}, "
                f"limit_px={self.price}, avg_px={self.avg_fill_price})")


# --- Worker Class (QObject for Qt Signals/Slots if running in Qt thread) ---
# If Worker runs in a non-Qt thread and needs to communicate with Qt,
# it should emit signals that are connected to slots in the Qt thread.
# QObject is necessary if the Worker itself will own QTimer instances
# and manage them within a Qt event loop (if worker is moved to a QThread).
# For now, assuming Worker might use QTimer and emit signals to UI.
class Worker(QObject):
    # PyQt Signals for UI updates from worker thread
    # These signals will be emitted by the worker and connected to slots in TradingDashboard
    log_message_signal = pyqtSignal(str) # For general log messages
    error_message_signal = pyqtSignal(str, str) # title, message
    order_update_signal = pyqtSignal(dict) # For order status changes, new orders etc.
    market_data_signal = pyqtSignal(dict) # For processed market data (if worker handles stream)
    active_orders_fetched_signal = pyqtSignal(list) # List of active OrderData dicts
    account_id_fetched_signal = pyqtSignal(str) # When account ID is fetched

    def __init__(self, schwab_client: Client, dashboard_ref: Any): # dashboard_ref for config access, status updates
        super().__init__() # Initialize QObject
        self.client = schwab_client
        self.dashboard = dashboard_ref # Reference to the main UI/dashboard for config and signals
        self.logger = logging.getLogger(__name__ + ".Worker")

        self.account_id: Optional[str] = None
        self.account_hash: Optional[str] = None # Often used as accountId in API calls
        self.orders: Dict[str, OrderData] = {} # client_order_id -> OrderData
        self.active_order_ids_on_broker: List[str] = [] # Stores order IDs reported by broker

        self.order_lock = Lock() # To protect access to self.orders
        self.api_call_timestamps = deque() # For rate limiting

        self.trading_enabled = False # Master switch for placing trades
        self.current_symbol_config: Optional[Dict[str, Any]] = None # Symbol specific params

        # Timer for periodic tasks like checking token validity, monitoring orders
        # These timers will only run if the Worker instance is moved to a QThread
        # and has its own event loop, or if these are managed by the main Qt event loop.
        # For now, let's define them. They would be started as needed.
        self.token_refresh_timer = QTimer(self)
        self.token_refresh_timer.timeout.connect(self.check_and_refresh_token)

        self.order_monitor_timer = QTimer(self)
        self.order_monitor_timer.timeout.connect(self.monitor_active_orders)

        self.logger.info("Worker initialized with full logic.")
        self.log_message_signal.emit("Worker initialized (full logic).")

    def start_worker_tasks(self):
        """Starts periodic tasks like token refresh and order monitoring."""
        if not self.account_id: # Account ID is crucial for many operations
            self.fetch_account_details() # Try to fetch it first

        if self.token_refresh_timer and not self.token_refresh_timer.isActive():
            self.token_refresh_timer.start(TOKEN_REFRESH_CHECK_INTERVAL_MS)
            self.logger.info(f"Token refresh check timer started (interval: {TOKEN_REFRESH_CHECK_INTERVAL_MS}ms).")

        if self.order_monitor_timer and not self.order_monitor_timer.isActive():
            self.order_monitor_timer.start(ORDER_MONITOR_INTERVAL_MS)
            self.logger.info(f"Order monitor timer started (interval: {ORDER_MONITOR_INTERVAL_MS}ms).")

    def stop_worker_tasks(self):
        """Stops all periodic tasks."""
        if self.token_refresh_timer and self.token_refresh_timer.isActive():
            self.token_refresh_timer.stop()
            self.logger.info("Token refresh check timer stopped.")
        if self.order_monitor_timer and self.order_monitor_timer.isActive():
            self.order_monitor_timer.stop()
            self.logger.info("Order monitor timer stopped.")
        self.log_message_signal.emit("Worker tasks stopped.")


    def _can_make_api_call(self) -> bool:
        """Checks if an API call can be made based on rate limits."""
        current_time = time.time()
        # Remove timestamps older than 60 seconds
        while self.api_call_timestamps and self.api_call_timestamps[0] <= current_time - 60:
            self.api_call_timestamps.popleft()

        if len(self.api_call_timestamps) < API_RATE_LIMIT_PER_MINUTE:
            self.api_call_timestamps.append(current_time)
            return True
        else:
            self.logger.warning("API rate limit likely exceeded. Call deferred.")
            self.error_message_signal.emit("API Limit", "Rate limit potentially exceeded. Try again shortly.")
            return False

    def fetch_account_details(self):
        """Fetches account numbers and sets the primary trading account ID/hash."""
        if not self._can_make_api_call():
            self.log_message_signal.emit("Cannot fetch account details due to API rate limit.")
            return

        self.log_message_signal.emit("Fetching account details...")
        try:
            response = self.client.get_account_numbers() # This method needs to exist in schwab.client.Client
            if response.ok:
                accounts_data = response.json()
                if accounts_data and isinstance(accounts_data, list) and accounts_data[0].get('hashValue'):
                    primary_account = accounts_data[0] # Assuming first account is primary
                    self.account_id = primary_account.get('accountNumber', 'N/A')
                    self.account_hash = primary_account.get('hashValue')
                    self.logger.info(f"Account ID: {self.account_id}, Account Hash: {self.account_hash} fetched.")
                    self.log_message_signal.emit(f"Account details fetched: ID {self.account_id[:4]}...")
                    self.account_id_fetched_signal.emit(self.account_hash) # Emit hash, often used as ID
                else:
                    self.logger.error(f"Failed to parse account details from response: {accounts_data}")
                    self.error_message_signal.emit("Account Error", "Could not parse account details.")
            else:
                self.logger.error(f"Failed to fetch account numbers: {response.status_code} - {response.text}")
                self.error_message_signal.emit("API Error", f"Failed to fetch accounts: {response.text}")
        except Exception as e:
            self.logger.error(f"Exception fetching account details: {e}", exc_info=True)
            self.error_message_signal.emit("System Error", f"Exception fetching accounts: {e}")


    def check_and_refresh_token(self):
        """Checks token validity and refreshes if necessary."""
        # This logic assumes client has methods like 'is_token_expired' and 'refresh_token'
        # Or, it might be handled by the client library automatically using `token_path`.
        # For robust GUI app, token path should be absolute and user-writable.
        self.logger.debug("Checking token status...")
        try:
            # Example: if self.client.is_token_near_expiry(buffer_seconds=TOKEN_REFRESH_BUFFER_SECONDS):
            # The schwab-py client library handles token refresh automatically if token_path is set
            # and write_tokens_to_file is True (default).
            # So, an explicit check/refresh might only be needed if that mechanism fails or for logging.

            # Let's assume the client has a way to check status or last refresh time.
            # For now, we'll rely on the client's internal mechanism.
            # If `token_path` is not writable or an error occurs, manual intervention might be needed.
            # This method can serve as a periodic check that the token mechanism is working.

            # A simple check could be trying a lightweight API call.
            # if not self.client.ensure_token_valid(): # Hypothetical method
            #    self.logger.warning("Token was invalid and refresh might have failed or is in progress.")
            #    self.error_message_signal.emit("Token Issue", "Token refresh might be needed or has failed.")
            # else:
            #    self.logger.info("Token is valid.")

            # For now, this is more of a placeholder unless specific logic for schwab-py is needed here.
            # The client object itself should manage this. We can log client's status.
            if hasattr(self.client, 'token_manager') and self.client.token_manager:
                 last_refresh = self.client.token_manager.last_refresh_time
                 if last_refresh:
                     elapsed_since_refresh = time.time() - last_refresh
                     self.logger.info(f"Token last refreshed {elapsed_since_refresh:.0f}s ago.")
                 else:
                     self.logger.info("Token refresh time unknown, relying on client's auto-refresh.")
            else:
                 self.logger.debug("Client does not have detailed token_manager or it's not initialized for this check.")


        except Exception as e:
            self.logger.error(f"Exception during token check/refresh: {e}", exc_info=True)
            self.error_message_signal.emit("Token Error", f"Exception during token management: {e}")
            # Consider retrying refresh after a delay if a specific refresh call failed here.
            # QTimer.singleShot(TOKEN_REFRESH_RETRY_DELAY_MS, self.check_and_refresh_token)


    def place_order(self, symbol: str, quantity: int, instruction: str, order_type: str, price: Optional[float] = None) -> Optional[str]:
        """Places an order using the Schwab API."""
        if not self.trading_enabled:
            self.log_message_signal.emit("Trading is disabled. Order not placed.")
            return None
        if not self._can_make_api_call():
            self.log_message_signal.emit("Cannot place order due to API rate limit.")
            return None
        if not self.account_hash:
            self.error_message_signal.emit("Order Error", "Account ID not set. Cannot place order.")
            self.fetch_account_details() # Attempt to fetch it
            return None

        # Validate parameters (basic)
        if not Validator.is_valid_symbol(symbol) or \
           not Validator.is_valid_quantity(quantity) or \
           (price is not None and not Validator.is_valid_price(price)):
            err_msg = f"Invalid order parameters for {symbol}, Qty:{quantity}, Px:{price}"
            self.logger.error(err_msg)
            self.error_message_signal.emit("Order Validation", err_msg)
            return None

        client_order_id = f"{instruction.lower()}_{symbol}_{int(time.time()*1000)}"

        # Construct order payload based on schwab-py client's place_order requirements
        # This is a simplified example and needs to match the actual client method signature.
        # The schwab-py library provides OrderBuilder for complex orders.
        # For simple equity orders:
        from schwab.orders.equities import equity_buy_market, equity_sell_market, equity_buy_limit, equity_sell_limit

        order_payload = None
        if order_type.upper() == "MARKET":
            if instruction.upper() == INSTR_BUY:
                order_payload = equity_buy_market(symbol, quantity)
            elif instruction.upper() == INSTR_SELL:
                order_payload = equity_sell_market(symbol, quantity)
        elif order_type.upper() == "LIMIT":
            if price is None:
                self.error_message_signal.emit("Order Error", "Price is required for LIMIT orders.")
                return None
            if instruction.upper() == INSTR_BUY:
                order_payload = equity_buy_limit(symbol, quantity, price)
            elif instruction.upper() == INSTR_SELL:
                order_payload = equity_sell_limit(symbol, quantity, price)

        if not order_payload:
            self.error_message_signal.emit("Order Error", f"Unsupported order type/instruction: {order_type}/{instruction}")
            return None

        # Add client order ID to the payload if supported by the builder/client
        # order_payload.set_client_order_id(client_order_id) # Hypothetical, check schwab-py docs

        self.log_message_signal.emit(f"Placing order: {instruction} {quantity} {symbol} @ {price or order_type}")
        try:
            # The actual place_order method might differ in schwab-py
            # r = self.client.place_order(self.account_hash, order_payload.build()) # Example if builder is used
            r = self.client.place_orders(self.account_hash, [order_payload]) # place_orders takes a list

            if r.ok: # Or check specific success status codes
                # Get the Schwab order ID from the response header (Location) or body
                # This depends on the API version and client library handling.
                # For schwab-py, order ID is usually in Location header for single orders.
                # For place_orders (bulk), it might be in the response body or individual order responses.

                schwab_order_id = r.headers.get('Location', '').split('/')[-1] if r.headers.get('Location') else None
                if not schwab_order_id and r.json(): # Check body if bulk or different response
                    # This part is speculative based on typical API patterns
                    responses = r.json()
                    if isinstance(responses, list) and responses:
                        schwab_order_id = responses[0].get('order_id') # Or similar key

                if not schwab_order_id: # If still no ID, it's a problem
                    self.logger.warning(f"Order placed for {client_order_id} but no Schwab Order ID found in response. Resp: {r.text}")
                    schwab_order_id = "UNKNOWN_" + client_order_id # Fallback ID

                self.logger.info(f"Order placed successfully: Client ID {client_order_id}, Schwab ID {schwab_order_id}")

                with self.order_lock:
                    new_order = OrderData(
                        order_id=schwab_order_id,
                        client_order_id=client_order_id,
                        symbol=symbol,
                        quantity=quantity,
                        price=price,
                        status=ORDER_STATUS_OPEN, # Or WORKING, based on API feedback
                        instruction=instruction.upper(),
                        raw_response=r.json() if r.content else None
                    )
                    self.orders[client_order_id] = new_order

                self.order_update_signal.emit(new_order.__dict__) # Emit new order details
                self.log_message_signal.emit(f"Order {schwab_order_id} submitted for {symbol}.")
                # Start monitoring this new order more closely if needed
                # self.monitor_single_order(schwab_order_id)
                return client_order_id
            else:
                err_msg = f"Order placement failed: {r.status_code} - {r.text}"
                self.logger.error(err_msg)
                self.error_message_signal.emit("Order API Error", err_msg)
                # Store failed order attempt if needed for audit
                return None
        except Exception as e:
            self.logger.error(f"Exception placing order: {e}", exc_info=True)
            self.error_message_signal.emit("Order System Error", f"Exception: {e}")
            return None

    def cancel_order(self, order_id_to_cancel: str) -> bool:
        """Cancels a specific order by its Schwab order ID."""
        if not self.trading_enabled: # Or a separate flag for cancellations
            self.log_message_signal.emit("Trading/cancellation is disabled. Order not cancelled.")
            return False
        if not self._can_make_api_call():
            self.log_message_signal.emit("Cannot cancel order due to API rate limit.")
            return False
        if not self.account_hash:
            self.error_message_signal.emit("Cancel Error", "Account ID not set. Cannot cancel order.")
            return False

        self.log_message_signal.emit(f"Attempting to cancel order ID: {order_id_to_cancel}")
        try:
            # schwab-py: client.cancel_order(account_hash, order_id)
            r = self.client.cancel_order(self.account_hash, order_id_to_cancel)
            if r.ok: # Or specific success code for cancellation
                self.logger.info(f"Order {order_id_to_cancel} cancellation request successful.")
                self.log_message_signal.emit(f"Order {order_id_to_cancel} cancellation submitted.")

                # Update local order status optimistically, or wait for ACCT_ACTIVITY
                with self.order_lock:
                    # Find by schwab_order_id
                    found_order_client_id = None
                    for cid, o_data in self.orders.items():
                        if o_data.order_id == order_id_to_cancel:
                            o_data.status = "PENDING_CANCEL" # Or CANCELED if API confirms immediately
                            found_order_client_id = cid
                            self.order_update_signal.emit(o_data.__dict__)
                            break
                return True
            else:
                err_msg = f"Order cancellation failed: {r.status_code} - {r.text}"
                self.logger.error(err_msg)
                self.error_message_signal.emit("Cancel API Error", err_msg)
                return False
        except Exception as e:
            self.logger.error(f"Exception cancelling order {order_id_to_cancel}: {e}", exc_info=True)
            self.error_message_signal.emit("Cancel System Error", f"Exception: {e}")
            return False

    def monitor_active_orders(self):
        """Periodically fetches status of all known active (non-terminal) orders."""
        if not self._can_make_api_call():
            self.logger.warning("Skipping order monitoring due to rate limit.")
            return
        if not self.account_hash:
            self.logger.warning("Skipping order monitoring, account ID not set.")
            if not self.account_id_fetched_signal._receivers: # Check if UI is waiting
                 self.fetch_account_details()
            return

        self.logger.debug("Monitoring active orders...")
        try:
            # schwab-py: client.get_orders_for_account(account_hash, query_controls)
            # Query for orders placed today, with statuses like WORKING, OPEN, PENDING_ACTIVATION etc.
            # This is a simplified query, actual API might need date ranges or specific status filters.
            from_entered_time = datetime.now() - timedelta(days=1) # Example: orders from last 24 hours
            to_entered_time = datetime.now()

            # Max results can be up to ORDER_ACTIVE_ORDERS_MAX or what API supports
            response = self.client.get_orders_for_account(
                self.account_hash,
                max_results=ORDER_ACTIVE_ORDERS_MAX,
                from_entered_datetime=from_entered_time,
                to_entered_datetime=to_entered_time,
                # status="WORKING" # Filter by status if API supports, or filter locally
            )

            if response.ok:
                live_orders_data = response.json()
                if not isinstance(live_orders_data, list):
                    self.logger.warning(f"Expected list of orders, got {type(live_orders_data)}. Skipping update.")
                    return

                self.active_order_ids_on_broker.clear()
                updated_order_details_for_ui = []

                for order_detail_from_api in live_orders_data:
                    api_order_id = str(order_detail_from_api.get('orderId', ''))
                    self.active_order_ids_on_broker.append(api_order_id)

                    # Try to find corresponding client_order_id (may not exist if order placed outside this app)
                    client_order_id = None
                    order_in_local_cache = None
                    with self.order_lock:
                        for cid, o_data_local in self.orders.items():
                            if str(o_data_local.order_id) == api_order_id:
                                client_order_id = cid
                                order_in_local_cache = o_data_local
                                break

                    new_status = order_detail_from_api.get('status', 'UNKNOWN')

                    if order_in_local_cache:
                        if order_in_local_cache.status != new_status:
                            self.logger.info(f"Status update for order {api_order_id} (Client ID: {client_order_id}): {order_in_local_cache.status} -> {new_status}")
                            order_in_local_cache.status = new_status
                            order_in_local_cache.filled_quantity = order_detail_from_api.get('filledQuantity', order_in_local_cache.filled_quantity)
                            # Update other fields like avg_fill_price if available
                            # order_in_local_cache.avg_fill_price = ...
                            order_in_local_cache.raw_response = order_detail_from_api # Update with latest full data
                            self.order_update_signal.emit(order_in_local_cache.__dict__)
                        updated_order_details_for_ui.append(order_in_local_cache.__dict__)
                    else:
                        # Order exists on broker but not in our local cache (e.g. placed via website)
                        # We can add it to our cache if we want to track it.
                        # For now, just log it.
                        self.logger.info(f"Found external active order on broker: ID {api_order_id}, Status {new_status}")
                        # To track it:
                        # new_external_order = OrderData(order_id=api_order_id, client_order_id="external_"+api_order_id, ...)
                        # self.orders["external_"+api_order_id] = new_external_order
                        # self.order_update_signal.emit(new_external_order.__dict__)
                        # updated_order_details_for_ui.append(new_external_order.__dict__)

                # Emit the full list of active orders from broker for UI to reconcile its display
                self.active_orders_fetched_signal.emit(updated_order_details_for_ui)

                # Check local orders that are no longer reported by broker (e.g. filled, cancelled and archived by broker)
                with self.order_lock:
                    for cid, o_data_local in list(self.orders.items()): # list() for safe iteration while modifying
                        if o_data_local.order_id not in self.active_order_ids_on_broker and \
                           o_data_local.status not in [ORDER_STATUS_FILLED, ORDER_STATUS_CANCELED, ORDER_STATUS_REJECTED, ORDER_STATUS_EXPIRED]:
                            self.logger.info(f"Local order {o_data_local.order_id} (Client ID: {cid}) no longer active on broker. Status was {o_data_local.status}. Re-querying specifically or marking stale.")
                            # Could mark as STALE or try to get its final status via get_order
                            # For now, let's assume ACCT_ACTIVITY stream will handle final status updates.
                            # If an order is truly gone without a final status, it's an issue.
                            pass


            else:
                self.logger.error(f"Failed to get active orders: {response.status_code} - {response.text}")
                self.error_message_signal.emit("Order Monitor Error", f"Failed to get orders: {response.text}")

        except Exception as e:
            self.logger.error(f"Exception in order monitoring: {e}", exc_info=True)
            self.error_message_signal.emit("System Error", f"Exception in order monitor: {e}")

    def get_order(self, order_id_to_fetch: str):
        """Fetches details for a single specific order by its Schwab order ID."""
        if not self._can_make_api_call() or not self.account_hash: return

        self.logger.info(f"Fetching details for order ID: {order_id_to_fetch}")
        try:
            # schwab-py: client.get_order(account_hash, order_id)
            r = self.client.get_order(self.account_hash, order_id_to_fetch)
            if r.ok:
                order_details = r.json()
                api_order_id = str(order_details.get('orderId', ''))
                client_order_id = None # Find if it's in our local cache
                updated_order = None
                with self.order_lock:
                    for cid, o_data_local in self.orders.items():
                        if str(o_data_local.order_id) == api_order_id:
                            client_order_id = cid
                            o_data_local.status = order_details.get('status', o_data_local.status)
                            o_data_local.filled_quantity = order_details.get('filledQuantity', o_data_local.filled_quantity)
                            # Update other relevant fields
                            o_data_local.raw_response = order_details
                            updated_order = o_data_local.__dict__
                            break
                if updated_order:
                    self.order_update_signal.emit(updated_order)
                    self.log_message_signal.emit(f"Order {api_order_id} details updated.")
                else: # Order not in local cache, but fetched (e.g. for audit)
                    self.log_message_signal.emit(f"Details for external order {api_order_id} fetched.")
                    # Potentially emit this too if UI needs to know about any fetched order
                    # self.order_update_signal.emit(order_details) # Emitting raw dict if not in local cache
            else:
                self.logger.error(f"Failed to get order {order_id_to_fetch}: {r.status_code} - {r.text}")
        except Exception as e:
            self.logger.error(f"Exception fetching order {order_id_to_fetch}: {e}", exc_info=True)


    def process_stream_message(self, message: Dict[Any, Any]):
        """Processes messages received from the WebSocket stream (e.g., ACCT_ACTIVITY)."""
        # This method would be called by the stream handler in TradingDashboard
        # which receives data from StreamClient.
        self.logger.debug(f"Worker processing stream message: {message}")

        # Assuming message is a list of data items, as per typical Schwab stream structure
        if not isinstance(message, list):
            self.logger.warning(f"Stream message is not a list: {message}")
            return

        for item in message:
            if not isinstance(item, dict): continue

            service = item.get('service', '').upper()
            timestamp_ms = item.get('timestamp') # Milliseconds

            if service == SERVICE_ACCT_ACTIVITY:
                content_list = item.get('content', [])
                for activity_item in content_list:
                    self.process_activity_message(activity_item, timestamp_ms)

            elif service == SERVICE_LEVELONE_EQUITIES:
                # If worker is also responsible for processing L1 quotes (e.g. for P&L, advanced logic)
                # For now, assuming UI handles L1 display directly, and worker gets relevant data via other means
                # or if strategy needs direct L1 feed.
                # content = item.get('content', [{}])[0]
                # symbol = content.get('key')
                # self.market_data_signal.emit({"type": "level1", "symbol": symbol, "data": content})
                pass
            else:
                self.logger.info(f"Worker received unhandled service message: {service}")


    def process_activity_message(self, activity_data: Dict[str, Any], timestamp_ms: int):
        """Processes a single message from the ACCT_ACTIVITY stream."""
        # Detailed parsing of ACCT_ACTIVITY messages. Structure can be complex.
        # Refer to Schwab API documentation for exact fields.
        # Example fields: MessageType, OrderId, ExecutionId, Status, Symbol, etc.

        message_type = activity_data.get('Type') # Or 'MESSAGE_TYPE', 'type' etc.
        self.logger.info(f"Processing ACCT_ACTIVITY: Type '{message_type}', Data: {activity_data}")

        # Example: Order updates (Execution Details, Order Confirmation, UROUT, Order Cancel/Replace messages)
        # Schwab uses specific message types like "UROUT", "ExecutionDetails", "OrderConfirmation"

        # This is a simplified interpretation. Real parsing needs to be robust.
        schwab_order_id = activity_data.get('OrderId') or activity_data.get('OrderKey') # Key might vary
        if not schwab_order_id:
            self.logger.debug(f"ACCT_ACTIVITY message without OrderId: {activity_data}")
            # Handle other activity types: Cash transfers, Dividends, Alerts etc.
            # self.log_message_signal.emit(f"Account Activity: {message_type} - {activity_data.get('MessageText', 'No details')}")
            return

        schwab_order_id = str(schwab_order_id)

        # Find our local order by Schwab Order ID
        order_to_update: Optional[OrderData] = None
        client_order_id_found = None
        with self.order_lock:
            for cid, o_data in self.orders.items():
                if str(o_data.order_id) == schwab_order_id:
                    order_to_update = o_data
                    client_order_id_found = cid
                    break

        if not order_to_update:
            self.logger.info(f"Received activity for order ID {schwab_order_id} not in local active cache (possibly external or already completed).")
            # Could fetch it via get_order if needed, or create a new OrderData entry.
            # For now, we'll just log and potentially emit a generic update if crucial.
            # If this is a fill for an order not tracked, it's important for position keeping.
            # A more robust system might create a temporary OrderData object here.
            # For now, only update if already known.
            return

        # Update order based on activity message
        # This is highly dependent on the specific fields in Schwab's ACCT_ACTIVITY messages.
        new_status = activity_data.get('Status') # e.g. "FILLED", "CANCELED"
        if new_status and order_to_update.status != new_status:
            order_to_update.status = new_status
            self.logger.info(f"Order {schwab_order_id} status updated to {new_status} via ACCT_ACTIVITY.")

        if new_status == ORDER_STATUS_FILLED:
            # Parse fill details: quantity, price, execution ID etc.
            # These field names are examples, check Schwab docs
            filled_qty_leg = activity_data.get('Quantity') or activity_data.get('FilledQuantity')
            fill_price_leg = activity_data.get('Price') or activity_data.get('ExecutionPrice')

            if filled_qty_leg is not None: order_to_update.filled_quantity = int(filled_qty_leg) # Or += if partial fills
            if fill_price_leg is not None: order_to_update.avg_fill_price = float(fill_price_leg) # Or calculate VWAP for partials

            self.logger.info(f"Order {schwab_order_id} FILLED. Qty: {filled_qty_leg}, Price: {fill_price_leg}")

        elif new_status == ORDER_STATUS_CANCELED:
            self.logger.info(f"Order {schwab_order_id} CANCELED via ACCT_ACTIVITY.")

        elif new_status == ORDER_STATUS_REJECTED:
            rejection_reason = activity_data.get('RejectReason', 'Unknown reason')
            self.logger.warning(f"Order {schwab_order_id} REJECTED via ACCT_ACTIVITY. Reason: {rejection_reason}")
            order_to_update.raw_response = activity_data # Store rejection details
            self.error_message_signal.emit("Order Rejected", f"Order {schwab_order_id} rejected: {rejection_reason}")

        # Update raw response and timestamp
        order_to_update.raw_response = activity_data
        order_to_update.timestamp = timestamp_ms / 1000.0 if timestamp_ms else time.time()

        self.order_update_signal.emit(order_to_update.__dict__)
        self.log_message_signal.emit(f"Order {schwab_order_id} updated via stream: Status {order_to_update.status}.")


    def set_trading_enabled_status(self, enabled: bool):
        self.trading_enabled = enabled
        self.logger.info(f"Trading master switch set to: {self.trading_enabled}")
        self.log_message_signal.emit(f"Trading {'ENABLED' if enabled else 'DISABLED'} by master control.")

    def set_current_symbol_config(self, config: Dict[str, Any]):
        self.current_symbol_config = config
        self.logger.info(f"Worker symbol config updated: {config.get('symbol')}")
        # Potentially (re)start symbol-specific logic or data subscriptions if worker manages them
        # For now, just stores it.

    def get_current_holdings(self, symbol: str) -> int:
        """ Placeholder to get current holdings for a symbol. Needs portfolio integration. """
        # This would typically involve fetching portfolio/positions data.
        # For now, returns a mock value or 0.
        self.logger.info(f"get_current_holdings for {symbol} called (placeholder - returns 0).")
        return 0 # Mock: no holdings

    # --- Methods called by UI actions (slots if Worker is QThreaded) ---
    def on_manual_buy_requested(self, symbol: str, quantity: int, order_type: str, price: Optional[float]):
        self.logger.info(f"Manual BUY requested: {quantity} {symbol} @ {price or order_type}")
        self.place_order(symbol, quantity, INSTR_BUY, order_type.upper(), price)

    def on_manual_sell_requested(self, symbol: str, quantity: int, order_type: str, price: Optional[float]):
        self.logger.info(f"Manual SELL requested: {quantity} {symbol} @ {price or order_type}")
        # Add check for holdings if selling, unless short selling is intended/allowed
        # current_holdings = self.get_current_holdings(symbol)
        # if quantity > current_holdings:
        #     self.error_message_signal.emit("Sell Error", f"Cannot sell {quantity} {symbol}, holdings: {current_holdings}")
        #     return
        self.place_order(symbol, quantity, INSTR_SELL, order_type.upper(), price)

    def on_cancel_order_requested(self, order_id: str): # Expects Schwab Order ID
        self.logger.info(f"Cancel order requested for Schwab Order ID: {order_id}")
        self.cancel_order(order_id)

    def on_cancel_all_orders_requested(self, symbol_to_cancel: Optional[str] = None):
        """Cancels all cancellable orders, optionally filtered by symbol."""
        self.logger.info(f"Cancel ALL orders requested (Symbol: {symbol_to_cancel or 'Any'}).")
        # Iterate through self.orders, find cancellable ones, and call self.cancel_order()
        # This needs careful implementation to avoid issues with partially filled orders, etc.
        # For now, this is a conceptual placeholder.
        # A simpler approach might be to use a bulk cancel API if available, or iterate known active orders.

        # Using locally cached order list:
        orders_to_attempt_cancel = []
        with self.order_lock:
            for client_id, o_data in self.orders.items():
                if o_data.status in [ORDER_STATUS_WORKING, ORDER_STATUS_OPEN, ORDER_STATUS_PENDING_ACTIVATION]: # Cancellable statuses
                    if symbol_to_cancel is None or o_data.symbol == symbol_to_cancel:
                        orders_to_attempt_cancel.append(o_data.order_id)

        if not orders_to_attempt_cancel:
            self.log_message_signal.emit(f"No active orders found to cancel for symbol '{symbol_to_cancel or 'Any'}'.")
            return

        for schwab_oid in orders_to_attempt_cancel:
            self.log_message_signal.emit(f"Attempting cancel for: {schwab_oid}")
            self.cancel_order(schwab_oid) # Rate limiting is handled by cancel_order
            time.sleep(0.1) # Small delay between cancel calls if making many

        self.log_message_signal.emit(f"Cancel all request processed for {len(orders_to_attempt_cancel)} order(s).")


    def cleanup_and_exit(self):
        """Prepares worker for application shutdown."""
        self.logger.info("Worker cleaning up and exiting...")
        self.stop_worker_tasks()
        # Join any running threads if worker managed its own non-Qt threads
        # (Currently, Worker uses QTimer, which is event-loop based)
        self.log_message_signal.emit("Worker shutdown complete.")
        self.logger.info("Worker shutdown procedures finished.")
