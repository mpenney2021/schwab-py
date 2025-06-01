import sys
import os
import json
import time
from datetime import datetime, timedelta # Added timedelta
import logging
import glob
import argparse
import tracemalloc
import cProfile
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QLabel, QLineEdit,
                             QPushButton, QGroupBox, QTextEdit, QGridLayout, QHBoxLayout,
                             QVBoxLayout, QTabWidget, QMessageBox, QCheckBox, QTimeEdit,
                             QDialog, QStatusBar, QComboBox)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QDateTime
from PyQt6.QtGui import QFont, QTextCursor
from collections import deque
from threading import Thread
from typing import Optional, Dict, Any, List

# Assuming schwab.client.Client and schwab.streaming.Stream will be available
# If schwab.client.Client is part of schwabdev, it might need adjustment later
# For now, let's use the paths as if they are directly under a 'schwab' package
# This might require moving the schwabdev library contents into the schwab package
# or adjusting sys.path if schwabdev is a separate top-level package.
# For this step, we'll assume they can be imported like this.
from schwab.client import Client
from schwab.streaming import Stream
from schwab.trading_logic import Worker, Validator # Import moved classes


# --- Constants ---
CONFIG_FILENAME = "config.json"
NOTEPAD_FILENAME = "notepad.txt"
LOG_FILENAME = "trading_app.log"
MAX_LOG_FILE_SIZE_MB = 5
LOG_BACKUP_COUNT = 3
UI_UPDATE_INTERVAL_MS = 100  # Update UI 10 times per second
LOG_QUEUE_MAXLEN = 1000
MAX_UI_LOG_LINES = 200

# Environment variable keys - keep here as load_environment_variables is here
APP_KEY_ENV = "APP_KEY"
APP_SECRET_ENV = "APP_SECRET"
CALLBACK_URL_ENV = "CALLBACK_URL"
TOKEN_FILE_PATH_ENV = "TOKEN_FILE_PATH" # Added, was missing in prompt but used by load_env

APP_KEY = ""
APP_SECRET = ""
CALLBACK_URL = ""
TOKEN_FILE_PATH = "token.json" # Default if not in env

# --- Globals ---
# To be populated by load_environment_variables
# These are here because load_environment_variables is here.
# They are used by TradingDashboard.
app_key_global = None
app_secret_global = None
callback_url_global = None
token_file_path_global = "token.json" # Default

# --- Logging Setup ---
# Minimal logger for now, will be enhanced by setup_logging
logger = logging.getLogger(__name__)
# Queue for log messages to be displayed in the UI
log_queue = deque(maxlen=LOG_QUEUE_MAXLEN)

class QtLogHandler(logging.Handler):
    def __init__(self, log_queue_ui):
        super().__init__()
        self.log_queue_ui = log_queue_ui

    def emit(self, record):
        log_entry = self.format(record)
        self.log_queue_ui.append(log_entry)

def setup_logging(log_filename, max_log_size_mb, backup_count, ui_log_queue):
    global logger
    logger = logging.getLogger() # Get root logger
    logger.setLevel(logging.INFO)

    # Console Handler
    # ch = logging.StreamHandler()
    # ch.setLevel(logging.DEBUG) # More verbose for console if needed
    # formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
    # ch.setFormatter(formatter)
    # logger.addHandler(ch) # Removed to simplify, use file and UI logs primarily

    # File Handler (Rotating)
    # Ensure the directory for the log file exists
    log_dir = os.path.dirname(log_filename)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError as e:
            print(f"Error creating log directory {log_dir}: {e}")
            # Fallback to current directory if log directory creation fails
            log_filename = os.path.basename(log_filename)


    rfh = logging.handlers.RotatingFileHandler(
        log_filename,
        maxBytes=max_log_size_mb * 1024 * 1024,
        backupCount=backup_count
    )
    rfh.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    rfh.setFormatter(file_formatter)
    logger.addHandler(rfh)

    # PyQt UI Log Handler
    qt_handler = QtLogHandler(ui_log_queue)
    qt_handler.setLevel(logging.INFO)
    ui_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    qt_handler.setFormatter(ui_formatter)
    logger.addHandler(qt_handler)

    logger.info("Logging initialized.")
    # print("Logging setup complete. Check log file and UI for messages.")


# --- Environment Variable Loading ---
def load_environment_variables():
    global app_key_global, app_secret_global, callback_url_global, token_file_path_global
    # Load from .env file if present (optional, for local development)
    # from dotenv import load_dotenv # Removed dotenv dependency for now
    # load_dotenv()

    app_key_global = os.getenv(APP_KEY_ENV)
    app_secret_global = os.getenv(APP_SECRET_ENV)
    callback_url_global = os.getenv(CALLBACK_URL_ENV)
    token_file_path_global = os.getenv(TOKEN_FILE_PATH_ENV, "token.json") # Default to token.json

    if not all([app_key_global, app_secret_global, callback_url_global]):
        message = (
            "CRITICAL ERROR: Environment variables APP_KEY, APP_SECRET, or CALLBACK_URL not set. "
            "Please set these variables before running the application. "
            "APP_KEY: Your Schwab Application Key. "
            "APP_SECRET: Your Schwab Application Secret. "
            "CALLBACK_URL: Your Schwab Application Callback URL (e.g., https://127.0.0.1). "
            f"TOKEN_FILE_PATH (Optional, defaults to {token_file_path_global}): Path to store the token."
        )
        logger.critical(message)
        # For GUI, show a message box. For console, print and exit.
        # This part might need adjustment depending on when/how this function is called in a GUI app.
        # If called before QApplication is initialized, QMessageBox might not work.
        # For now, assume it's called at a point where logging is set up.
        # The application will likely fail to initialize the client later if these are missing.
        # A more robust GUI app would show this in a dialog before full UI init.
        # msg_box = QMessageBox()
        # msg_box.setIcon(QMessageBox.Icon.Critical)
        # msg_box.setText("Environment Variables Missing")
        # msg_box.setInformativeText(message)
        # msg_box.setWindowTitle("Configuration Error")
        # msg_box.exec() # This would block if called too early
        # sys.exit(1) # Exit if critical vars are missing. Consider how GUI handles this.
        print(message) # Print for now, GUI handling can be improved
        # Raising an exception might be better for the GUI to catch and display
        # raise EnvironmentError(message)
    else:
        logger.info("Successfully loaded environment variables for App Key and Token Path.")
        # print("Successfully loaded environment variables.") # Redundant with logger

# --- Trading Dashboard UI ---
class TradingDashboard(QMainWindow):
    # Signal to update log in UI thread
    log_updated_signal = pyqtSignal(str)
    # Signal for other UI updates from worker/stream
    generic_ui_update_signal = pyqtSignal(dict)


    def __init__(self, client, stream_client_factory, worker_factory):
        super().__init__()
        self.client = client
        self.stream_client_factory = stream_client_factory # To create StreamClient instance
        self.stream_client = None # Will be instantiated by stream_client_factory
        self.worker_factory = worker_factory # To create Worker instance
        self.worker = None # Will be instantiated by worker_factory

        self.current_bid = 0.0
        self.current_ask = 0.0
        self.current_price = 0.0 # Last trade price
        self.account_id = None # To be fetched or configured
        self.order_id_map = {} # Maps client_order_id to actual order_id
        self.active_orders = {} # Stores active order details: {order_id: order_data}
        self.config = self.load_config()
        self.is_streaming = False
        self.stream_thread = None
        self.trading_enabled = False # Master switch for trading actions

        self.init_ui()
        self.load_config_to_ui()
        self.load_notepad()

        # Connect signal for logging
        self.log_updated_signal.connect(self.append_log_message_to_ui)

        # Timer for various UI updates (e.g., clock, processing queued logs)
        self.ui_update_timer = QTimer(self)
        self.ui_update_timer.timeout.connect(self.process_log_queue)
        self.ui_update_timer.timeout.connect(self.update_clock)
        # self.ui_update_timer.timeout.connect(self.refresh_active_orders_display) # If needed periodically
        self.ui_update_timer.start(UI_UPDATE_INTERVAL_MS)

        # Initialize Worker (if factory provided)
        if self.worker_factory:
            self.worker = self.worker_factory(self.client, self) # Pass dashboard instance
            if self.worker:
                logger.info("Worker instance created successfully.")
                # Connect new worker signals to new specific slots
                self.worker.log_message_signal.connect(self.append_log_message_to_ui) # Worker logs directly to UI
                self.worker.error_message_signal.connect(self.handle_worker_error_message)
                self.worker.order_update_signal.connect(self.handle_worker_order_update)
                self.worker.active_orders_fetched_signal.connect(self.handle_worker_active_orders_fetched)
                self.worker.account_id_fetched_signal.connect(self.handle_worker_account_id_fetched)

                # Call method to start worker's internal timers/tasks if it has one
                if hasattr(self.worker, 'start_worker_tasks'):
                    self.worker.start_worker_tasks()

                # The generic_ui_update_signal is still used by handle_stream_data for market data
                # and potentially other non-worker UI updates.
                # Parts of handle_worker_update might become obsolete if all worker comms use specific signals.
                self.generic_ui_update_signal.connect(self.handle_generic_ui_update)
            else:
                logger.error("Worker factory failed to create a worker instance.")
                QMessageBox.critical(self, "Startup Error", "Failed to create the Trading Worker component.")
        else:
            logger.warning("Worker factory not provided to TradingDashboard. Some functionalities might be limited.")

        logger.info("TradingDashboard initialized.")
        self.statusBar().showMessage("Ready. Load symbols and start stream.")


    def init_ui(self):
        self.setWindowTitle("Schwab Trading Scalper Dashboard")
        self.setGeometry(100, 100, 1200, 800) # x, y, width, height

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)

        # Left Panel: Controls, Parameters, Notepad
        left_panel_layout = QVBoxLayout()

        # --- Parameters Group ---
        params_group = QGroupBox("Parameters")
        params_layout = QGridLayout()

        # Symbol
        params_layout.addWidget(QLabel("Symbol:"), 0, 0)
        self.symbol_input = QLineEdit("SPY")
        self.symbol_input.setToolTip("Enter stock symbol (e.g., SPY)")
        params_layout.addWidget(self.symbol_input, 0, 1)

        # Target Profit
        params_layout.addWidget(QLabel("Target Profit ($):"), 1, 0)
        self.target_profit_input = QLineEdit("0.05")
        self.target_profit_input.setToolTip("Desired profit per share for sell order")
        params_layout.addWidget(self.target_profit_input, 1, 1)

        # Stop Loss Offset
        params_layout.addWidget(QLabel("Stop Loss Offset ($):"), 2, 0)
        self.stop_loss_offset_input = QLineEdit("0.10")
        self.stop_loss_offset_input.setToolTip("Offset from buy price for stop loss order")
        params_layout.addWidget(self.stop_loss_offset_input, 2, 1)

        # Quantity
        params_layout.addWidget(QLabel("Quantity:"), 3, 0)
        self.quantity_input = QLineEdit("1")
        self.quantity_input.setToolTip("Number of shares per trade")
        params_layout.addWidget(self.quantity_input, 3, 1)

        # Start Time
        params_layout.addWidget(QLabel("Start Time (HH:MM:SS):"), 4, 0)
        self.start_time_input = QTimeEdit()
        self.start_time_input.setDisplayFormat("HH:mm:ss")
        self.start_time_input.setToolTip("Time to start automated trading (e.g., 09:30:00)")
        params_layout.addWidget(self.start_time_input, 4, 1)

        # End Time
        params_layout.addWidget(QLabel("End Time (HH:MM:SS):"), 5, 0)
        self.end_time_input = QTimeEdit()
        self.end_time_input.setDisplayFormat("HH:mm:ss")
        self.end_time_input.setToolTip("Time to stop automated trading (e.g., 16:00:00)")
        params_layout.addWidget(self.end_time_input, 5, 1)

        # Update Params Button
        self.update_params_button = QPushButton("Update Parameters")
        self.update_params_button.clicked.connect(self.update_params)
        self.update_params_button.setToolTip("Apply the parameters above")
        params_layout.addWidget(self.update_params_button, 6, 0, 1, 2)

        params_group.setLayout(params_layout)
        left_panel_layout.addWidget(params_group)

        # --- Control Group ---
        control_group = QGroupBox("Controls")
        control_layout = QVBoxLayout()

        self.load_symbol_button = QPushButton("Load Symbol & Connect Stream")
        self.load_symbol_button.clicked.connect(self.load_symbol_and_stream)
        self.load_symbol_button.setToolTip("Load symbol, fetch initial data, and start streaming quotes.")
        control_layout.addWidget(self.load_symbol_button)

        self.enable_trading_button = QPushButton("Enable Trading")
        self.enable_trading_button.setCheckable(True)
        self.enable_trading_button.clicked.connect(self.toggle_trading)
        self.enable_trading_button.setToolTip("Master switch to enable/disable placing trades.")
        control_layout.addWidget(self.enable_trading_button)

        # Renamed original manual buttons
        self.manual_buy_at_market_button = QPushButton("Manual Buy Market")
        self.manual_buy_at_market_button.clicked.connect(self.manual_buy_at_market) # Renamed slot
        self.manual_buy_at_market_button.setToolTip("Manually place a BUY MARKET order using current ask as indicative price.")
        control_layout.addWidget(self.manual_buy_at_market_button)

        self.manual_sell_at_market_button = QPushButton("Manual Sell Market")
        self.manual_sell_at_market_button.clicked.connect(self.manual_sell_all_at_market) # Renamed slot
        self.manual_sell_at_market_button.setToolTip("Manually sell all shares of the current symbol as a MARKET order using current bid as indicative price.")
        control_layout.addWidget(self.manual_sell_at_market_button)

        # --- New Manual Order Inputs ---
        control_layout.addWidget(QLabel("Order Type:"))
        self.manual_order_type_combo = QComboBox()
        self.manual_order_type_combo.addItems(["LIMIT", "MARKET"])
        self.manual_order_type_combo.setToolTip("Select order type for manual trade.")
        self.manual_order_type_combo.currentTextChanged.connect(self.update_manual_price_input_state)
        control_layout.addWidget(self.manual_order_type_combo)

        control_layout.addWidget(QLabel("Price:"))
        self.manual_price_input = QLineEdit("0.00")
        self.manual_price_input.setPlaceholderText("0.00")
        self.manual_price_input.setToolTip("Enter limit price (if LIMIT order type).")
        control_layout.addWidget(self.manual_price_input)

        self.generic_manual_buy_button = QPushButton("Place Manual Buy")
        self.generic_manual_buy_button.clicked.connect(self.handle_generic_manual_buy)
        self.generic_manual_buy_button.setToolTip("Place a manual buy order using specified type/price.")
        control_layout.addWidget(self.generic_manual_buy_button)

        self.generic_manual_sell_button = QPushButton("Place Manual Sell")
        self.generic_manual_sell_button.clicked.connect(self.handle_generic_manual_sell)
        self.generic_manual_sell_button.setToolTip("Place a manual sell order using specified type/price.")
        control_layout.addWidget(self.generic_manual_sell_button)
        # --- End New Manual Order Inputs ---

        self.cancel_all_orders_button = QPushButton("Cancel All Orders")
        self.cancel_all_orders_button.clicked.connect(self.cancel_all_active_orders)
        self.cancel_all_orders_button.setToolTip("Cancel all open orders for the current symbol.")
        control_layout.addWidget(self.cancel_all_orders_button)

        control_group.setLayout(control_layout)
        left_panel_layout.addWidget(control_group)

        # --- Notepad Group ---
        notepad_group = QGroupBox("Notepad")
        notepad_layout = QVBoxLayout()
        self.notepad_area = QTextEdit()
        self.notepad_area.setPlaceholderText("Scratchpad for notes...")
        self.save_notepad_button = QPushButton("Save Notepad")
        self.save_notepad_button.clicked.connect(self.save_notepad)
        notepad_layout.addWidget(self.notepad_area)
        notepad_layout.addWidget(self.save_notepad_button)
        notepad_group.setLayout(notepad_layout)
        left_panel_layout.addWidget(notepad_group)

        left_panel_layout.addStretch(1) # Pushes everything up

        # Right Panel: Tabs for Logs, Orders, Charts (placeholder)
        right_panel_layout = QVBoxLayout()
        self.tabs = QTabWidget()

        # Log Tab
        self.log_tab = QWidget()
        log_layout = QVBoxLayout(self.log_tab)
        self.log_area = QTextEdit()
        self.log_area.setReadOnly(True)
        self.log_area.setFont(QFont("Courier", 9))
        log_layout.addWidget(self.log_area)
        self.tabs.addTab(self.log_tab, "Logs")

        # Orders Tab
        self.orders_tab = QWidget()
        orders_layout = QVBoxLayout(self.orders_tab)
        # Placeholder for active orders display
        self.active_orders_display = QTextEdit() # Simple display for now
        self.active_orders_display.setReadOnly(True)
        self.active_orders_display.setFont(QFont("Courier", 9))
        self.active_orders_display.setPlaceholderText("Active orders will be shown here.")
        orders_layout.addWidget(QLabel("Active Orders:"))
        orders_layout.addWidget(self.active_orders_display)
        # Placeholder for order history (could be another QTextEdit or QTableView)
        self.order_history_display = QTextEdit()
        self.order_history_display.setReadOnly(True)
        self.order_history_display.setFont(QFont("Courier", 9))
        self.order_history_display.setPlaceholderText("Order history will be shown here.")
        orders_layout.addWidget(QLabel("Order History:"))
        orders_layout.addWidget(self.order_history_display)
        self.tabs.addTab(self.orders_tab, "Orders")

        # Chart Tab (Placeholder)
        self.chart_tab = QWidget()
        chart_layout = QVBoxLayout(self.chart_tab)
        chart_layout.addWidget(QLabel("Charting functionality will be here."))
        self.tabs.addTab(self.chart_tab, "Chart")

        right_panel_layout.addWidget(self.tabs)

        # Market Data Display (below parameters, above controls or integrated)
        market_data_group = QGroupBox("Market Data")
        market_data_layout = QGridLayout()
        market_data_layout.addWidget(QLabel("Current Symbol:"), 0, 0)
        self.current_symbol_display = QLabel("N/A")
        market_data_layout.addWidget(self.current_symbol_display, 0, 1)
        market_data_layout.addWidget(QLabel("Bid:"), 1, 0)
        self.bid_price_label = QLabel("N/A")
        market_data_layout.addWidget(self.bid_price_label, 1, 1)
        market_data_layout.addWidget(QLabel("Ask:"), 2, 0)
        self.ask_price_label = QLabel("N/A")
        market_data_layout.addWidget(self.ask_price_label, 2, 1)
        market_data_layout.addWidget(QLabel("Last:"), 3, 0)
        self.last_price_label = QLabel("N/A")
        market_data_layout.addWidget(self.last_price_label, 3, 1)
        market_data_group.setLayout(market_data_layout)
        left_panel_layout.insertWidget(1, market_data_group) # Insert after params

        # Clock display
        self.clock_label = QLabel(QDateTime.currentDateTime().toString("HH:mm:ss"))
        self.clock_label.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        # Add clock to status bar or a prominent place
        # For now, let's add it to the left panel for visibility
        left_panel_layout.addWidget(self.clock_label, 0, Qt.AlignmentFlag.AlignRight)


        main_layout.addLayout(left_panel_layout, 1) # Weight 1
        main_layout.addLayout(right_panel_layout, 3) # Weight 3 (larger)

        self.setStatusBar(QStatusBar())
        self.statusBar().showMessage("Application Initialized. Configure parameters and load symbol.")

        self.update_ui_element_states()


    def update_clock(self):
        self.clock_label.setText(QDateTime.currentDateTime().toString("HH:mm:ss"))

    def append_log_message_to_ui(self, message):
        """Appends a single log message to the UI log area."""
        self.log_area.append(message)
        if self.log_area.document().lineCount() > MAX_UI_LOG_LINES * 1.2: # Prune if it grows too large
            cursor = self.log_area.textCursor()
            cursor.movePosition(QTextCursor.MoveOperation.Start)
            for _ in range(int(MAX_UI_LOG_LINES * 0.2)): # Remove oldest 20%
                 if cursor.block().isValid():
                    cursor.select(QTextCursor.SelectionType.BlockUnderCursor)
                    cursor.removeSelectedText()
                 else: # Should not happen if lines > 0
                    break
            cursor.movePosition(QTextCursor.MoveOperation.End) # Ensure scroll to bottom
            self.log_area.setTextCursor(cursor)
        self.log_area.ensureCursorVisible()


    def process_log_queue(self):
        """Processes messages from the shared log queue and updates the UI."""
        # Process a few messages at a time to keep UI responsive
        count = 0
        while log_queue and count < 20: # Process up to 20 messages per interval
            try:
                message = log_queue.popleft()
                self.append_log_message_to_ui(message) # Directly update UI from main thread
                count += 1
            except IndexError: # Queue empty
                break

    def update_ui_element_states(self):
        """Enable/disable UI elements based on application state."""
        is_symbol_loaded = self.current_symbol_display.text() != "N/A"
        is_streaming_active = self.is_streaming
        is_trading_allowed = self.enable_trading_button.isChecked()

        self.symbol_input.setEnabled(not is_streaming_active)
        self.load_symbol_button.setEnabled(not is_streaming_active)
        # self.load_symbol_button.setText("Disconnect Stream" if is_streaming_active else "Load Symbol & Connect Stream")


        self.enable_trading_button.setEnabled(is_streaming_active)
        self.manual_buy_at_market_button.setEnabled(is_streaming_active and is_trading_allowed)
        self.manual_sell_at_market_button.setEnabled(is_streaming_active and is_trading_allowed) # Add logic for holding shares

        # New manual trade controls
        self.manual_order_type_combo.setEnabled(is_streaming_active and is_trading_allowed)
        # Price input state is also managed by update_manual_price_input_state
        is_limit_order = self.manual_order_type_combo.currentText() == "LIMIT"
        self.manual_price_input.setEnabled(is_streaming_active and is_trading_allowed and is_limit_order)
        self.generic_manual_buy_button.setEnabled(is_streaming_active and is_trading_allowed)
        self.generic_manual_sell_button.setEnabled(is_streaming_active and is_trading_allowed)

        self.cancel_all_orders_button.setEnabled(is_streaming_active) # Can always cancel

        # Parameter inputs - generally always editable unless mid-operation
        # Might disable them if trading is very active and params shouldn't change. For now, keep enabled.
        # self.target_profit_input.setEnabled(not is_trading_allowed) # Example

        if not is_streaming_active:
            self.statusBar().showMessage("Stream disconnected. Load a symbol to start.")
        elif not is_trading_allowed:
            self.statusBar().showMessage(f"Symbol {self.config.get('symbol', 'N/A')} loaded. Trading DISABLED.")
        else:
             self.statusBar().showMessage(f"Symbol {self.config.get('symbol', 'N/A')} loaded. Trading ENABLED.")


    def load_config(self):
        try:
            if os.path.exists(CONFIG_FILENAME):
                with open(CONFIG_FILENAME, 'r') as f:
                    config = json.load(f)
                    logger.info(f"Configuration loaded from {CONFIG_FILENAME}")
                    return config
            else:
                logger.info(f"{CONFIG_FILENAME} not found. Using default values.")
                # Return a default config structure if file doesn't exist
                return {
                    "symbol": "SPY", "target_profit": 0.05, "stop_loss_offset": 0.10,
                    "quantity": 1, "start_time": "09:30:00", "end_time": "16:00:00"
                }
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            QMessageBox.warning(self, "Config Error", f"Could not load config: {e}")
            return {} # Return empty or default config on error

    def save_config(self):
        try:
            with open(CONFIG_FILENAME, 'w') as f:
                json.dump(self.config, f, indent=4)
            logger.info(f"Configuration saved to {CONFIG_FILENAME}")
            self.statusBar().showMessage("Configuration saved.")
        except Exception as e:
            logger.error(f"Error saving config: {e}")
            QMessageBox.critical(self, "Config Error", f"Could not save config: {e}")
            self.statusBar().showMessage("Error saving configuration.")


    def load_config_to_ui(self):
        self.symbol_input.setText(self.config.get("symbol", "SPY"))
        self.target_profit_input.setText(str(self.config.get("target_profit", 0.05)))
        self.stop_loss_offset_input.setText(str(self.config.get("stop_loss_offset", 0.10)))
        self.quantity_input.setText(str(self.config.get("quantity", 1)))

        start_time_str = self.config.get("start_time", "09:30:00")
        end_time_str = self.config.get("end_time", "16:00:00")

        self.start_time_input.setTime(QDateTime.fromString(start_time_str, "HH:mm:ss").time())
        self.end_time_input.setTime(QDateTime.fromString(end_time_str, "HH:mm:ss").time())
        logger.info("Config values populated to UI.")


    def update_params(self):
        symbol = self.symbol_input.text().upper()
        target_profit_str = self.target_profit_input.text()
        stop_loss_offset_str = self.stop_loss_offset_input.text()
        quantity_str = self.quantity_input.text()
        start_time = self.start_time_input.time().toString("HH:mm:ss")
        end_time = self.end_time_input.time().toString("HH:mm:ss")

        # Validation
        errors = []
        if not Validator.is_valid_symbol(symbol):
            errors.append("Invalid symbol format.")
        if not Validator.is_valid_price(target_profit_str):
            errors.append("Invalid target profit value.")
        if not Validator.is_valid_price(stop_loss_offset_str): # Price validation for offset
            errors.append("Invalid stop loss offset value.")
        if not Validator.is_valid_quantity(quantity_str):
            errors.append("Invalid quantity value.")
        # Time validation is implicitly handled by QTimeEdit, but good to have if parsing from string elsewhere

        if errors:
            QMessageBox.warning(self, "Parameter Error", "\n".join(errors))
            logger.warning(f"Parameter validation failed: {errors}")
            return

        self.config["symbol"] = symbol
        self.config["target_profit"] = float(target_profit_str)
        self.config["stop_loss_offset"] = float(stop_loss_offset_str)
        self.config["quantity"] = int(quantity_str)
        self.config["start_time"] = start_time
        self.config["end_time"] = end_time

        self.save_config()
        logger.info(f"Parameters updated and saved: {self.config}")
        QMessageBox.information(self, "Parameters Updated", "Parameters have been updated and saved.")

        # If symbol changed and stream is active, offer to restart stream
        if self.is_streaming and self.current_symbol_display.text() != symbol:
            reply = QMessageBox.question(self, "Symbol Changed",
                                         "The symbol has changed. Do you want to disconnect the current stream and load the new symbol?",
                                         QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if reply == QMessageBox.StandardButton.Yes:
                self.disconnect_stream() # Disconnect first
                self.load_symbol_and_stream() # Then load new one
            else:
                # User chose not to restart, revert symbol in UI to current streaming symbol or warn
                self.symbol_input.setText(self.current_symbol_display.text())
                logger.info("Symbol change aborted by user. Stream continues with old symbol.")
                QMessageBox.information(self, "Stream Unchanged", f"Stream continues with symbol {self.current_symbol_display.text()}. Update symbol again if needed or disconnect manually.")
        self.update_ui_element_states()


    def load_notepad(self):
        try:
            if os.path.exists(NOTEPAD_FILENAME):
                with open(NOTEPAD_FILENAME, 'r') as f:
                    self.notepad_area.setPlainText(f.read())
                logger.info("Notepad content loaded.")
        except Exception as e:
            logger.error(f"Error loading notepad: {e}")
            self.notepad_area.setPlaceholderText("Could not load previous notes.")

    def save_notepad(self):
        try:
            with open(NOTEPAD_FILENAME, 'w') as f:
                f.write(self.notepad_area.toPlainText())
            logger.info("Notepad content saved.")
            self.statusBar().showMessage("Notepad saved.")
        except Exception as e:
            logger.error(f"Error saving notepad: {e}")
            QMessageBox.critical(self, "Notepad Error", f"Could not save notepad: {e}")
            self.statusBar().showMessage("Error saving notepad.")

    def get_current_params(self):
        """Returns a dictionary of the current trading parameters from the UI/config."""
        # Ensure config is up-to-date with UI before returning, or read directly from UI after validation
        # For simplicity, assuming self.config is the source of truth after "Update Parameters" is clicked
        return self.config.copy() # Return a copy

    def handle_stream_data(self, data_list):
        """
        Processes incoming data from the stream.
        This method is called by the StreamClient (likely from a different thread).
        It should emit signals to update the UI safely.
        """
        # Example: data_list = [{'service': 'QUOTE', 'symbol': 'SPY', 'bid': 123.45, 'ask': 123.50, 'last': 123.48, ...}]
        # logger.debug(f"GUI received stream data: {data_list}") # Can be very verbose
        for data_item in data_list:
            if not data_item or 'service' not in data_item:
                logger.warning(f"Received empty or malformed data item: {data_item}")
                continue

            service = data_item.get('service', '').upper()
            # timestamp = data_item.get('timestamp', time.time() * 1000) # Assuming ms timestamp

            if service == "QUOTE" or service == "LEVELONE_EQUITIES": # Handle both potential service names for quotes
                content = data_item.get('content', [{}])[0] # Data is often nested
                symbol = content.get('key', self.config.get('symbol')) # key is usually the symbol

                if symbol != self.config.get('symbol'): # Ensure data is for the subscribed symbol
                    # logger.warning(f"Received quote for {symbol}, expected {self.config.get('symbol')}. Ignoring.")
                    continue

                self.current_bid = float(content.get('BID_PRICE', 0))
                self.current_ask = float(content.get('ASK_PRICE', 0))
                self.current_price = float(content.get('LAST_PRICE', 0)) # Or REG_MARKET_LAST_PRICE etc.

                # Emit a signal with the new data for UI update
                update_payload = {
                    "type": "market_data",
                    "symbol": symbol,
                    "bid": self.current_bid,
                    "ask": self.current_ask,
                    "last": self.current_price
                }
                self.generic_ui_update_signal.emit(update_payload)


                # If worker is present and trading is enabled, pass data to worker
                if self.worker and self.trading_enabled:
                    # Worker might have its own logic for these updates
                    # self.worker.on_market_data_update(symbol, self.current_bid, self.current_ask, self.current_price)
                    # Or, more generically, pass the whole item if worker expects it
                    self.worker.handle_event({"type": "market_update", "data": content})


            elif service == "ACCT_ACTIVITY":
                logger.info(f"Account Activity Update: {data_item}")
                # This data can be complex, parse according to Schwab schema
                # Example: order fill notifications, etc.
                # Pass to worker or handle directly for UI updates (e.g. order status)
                if self.worker:
                    self.worker.handle_event({"type": "account_activity", "data": data_item})
                else: # Basic UI update if no worker
                    activity_type = data_item.get('content', [{}])[0].get('Type', 'Unknown Activity')
                    activity_msg = data_item.get('content', [{}])[0].get('Message', 'No details')
                    self.generic_ui_update_signal.emit({
                        "type": "account_activity_ui",
                        "message": f"Acct Activity: {activity_type} - {activity_msg}"
                    })


            elif service == "STREAMER_HEARTBEAT":
                # logger.debug(f"Streamer Heartbeat received: {data_item.get('content', [{}])[0]}")
                pass # Usually just indicates connection is alive

            else:
                logger.info(f"Unhandled stream service {service}: {data_item}")

    # Renamed from handle_worker_update to handle_generic_ui_update as it's no longer solely for worker messages
    def handle_generic_ui_update(self, update_data: Dict[str, Any]):
        """Handles generic UI updates, primarily market data from stream now."""
        update_type = update_data.get("type")
        # logger.debug(f"GUI received generic_ui_update: {update_type} - {update_data}")

        if update_type == "market_data": # This comes from handle_stream_data
            self.bid_price_label.setText(f"{update_data.get('bid', 0.0):.2f}")
            self.ask_price_label.setText(f"{update_data.get('ask', 0.0):.2f}")
            self.last_price_label.setText(f"{update_data.get('last', 0.0):.2f}")
            # Potentially update chart data here too

        # TODO: Review if "account_activity_ui" type is still needed here or if worker handles all account activity
        elif update_type == "account_activity_ui": # From basic stream handling if no worker
            self.order_history_display.append(f"[{QDateTime.currentDateTime().toString('HH:mm:ss')}] {update_data.get('message')}")

        # Other types previously handled here (order_status_update, active_orders_list, error_message, info_message)
        # are now largely handled by specific slots connected to worker signals.
        # This 'info_message' type might still be used by UI components directly.
        elif update_type == "info_message":
            info_msg = update_data.get("message", "")
            source = update_data.get("source", "UI") # Assume UI if not specified
            logger.info(f"Info ({source}): {info_msg}")
            self.statusBar().showMessage(info_msg, 3000) # Show for 3s

        # No longer expecting these from generic signal if worker uses specific signals:
        # "order_status_update", "active_orders_list", "error_message" (from worker)

    # --- New slots for specific Worker signals ---
    def handle_worker_error_message(self, title: str, message: str):
        """Handles error messages emitted by the worker."""
        QMessageBox.warning(self, title, message)
        logger.error(f"Error from Worker ({title}): {message}")
        self.statusBar().showMessage(f"Worker Error: {message[:50]}...", 5000)

    def handle_worker_order_update(self, order_data_dict: dict):
        """Handles order status updates from the worker."""
        # This data_dict is expected to be from OrderData.__dict__
        order_id = order_data_dict.get("order_id")
        client_order_id = order_data_dict.get("client_order_id")
        status = order_data_dict.get("status")

        logger.info(f"Order Update (from Worker Signal): ID {order_id or client_order_id}, Status: {status}, Data: {order_data_dict}")
        self.statusBar().showMessage(f"Order {order_id or client_order_id}: {status}")

        # Use the same UI update logic as before
        self.update_active_order_in_ui(order_data_dict)
        if status in [
            "FILLED", "CANCELED", "REJECTED", "EXPIRED", # Standard terminal statuses
            "Order Fills", "UROUT", "Cancel Confirmation" # Example text statuses from some APIs
        ]: # TODO: Align with constants from trading_logic.py
            self.move_order_to_history_in_ui(order_id or client_order_id, order_data_dict)

    def handle_worker_active_orders_fetched(self, active_orders_list: list):
        """Handles the list of active orders fetched by the worker."""
        # active_orders_list contains dicts from OrderData.__dict__
        self.active_orders.clear()
        for order_data_dict in active_orders_list:
            # The list might contain full OrderData objects or their dict representations.
            # Worker's active_orders_fetched_signal emits list of dicts.
            o_id = str(order_data_dict.get("order_id")) # Ensure key matches OrderData attribute
            if o_id:
                self.active_orders[o_id] = order_data_dict # Store the dict directly
        self.refresh_active_orders_display()
        logger.info(f"UI updated with {len(active_orders_list)} active orders from worker.")

    def handle_worker_account_id_fetched(self, account_hash: str):
        """Handles the account ID/hash fetched by the worker."""
        self.account_id = account_hash # Store the hash, often used as accountId in API calls
        logger.info(f"Account Hash/ID received from worker: {account_hash}")
        self.statusBar().showMessage(f"Account ID set: {account_hash[:10]}...")
        # Potentially enable UI elements that depend on account_id
        self.update_ui_element_states()


        # Add more update types as needed (e.g., portfolio updates, P&L updates)
        # self.update_ui_element_states() # Refresh states if anything significant changed


    def update_active_order_in_ui(self, order_data_dict: dict): # Changed param name for clarity
        # order_data_dict is now consistently a dictionary, likely from OrderData.__dict__
        order_id_str = str(order_data_dict.get("order_id") or order_data_dict.get("client_order_id"))
        if not order_id_str:
            logger.warning("update_active_order_in_ui: order_id missing from data.")
            return

        # self.active_orders now stores dictionaries directly
        if order_id_str in self.active_orders:
            self.active_orders[order_id_str].update(order_data_dict) # Merge updates into the existing dict
        else:
            self.active_orders[order_id_str] = order_data_dict # Add new dict

        self.refresh_active_orders_display()

    def move_order_to_history_in_ui(self, order_id_key_str: str, order_data_dict: dict): # Changed param names
        order_id_key_str = str(order_id_key_str) # Ensure it's a string key
        if order_id_key_str in self.active_orders:
            final_order_data = self.active_orders.pop(order_id_key_str) # Pop returns the dict
            final_order_data.update(order_data_dict) # Ensure it has the latest status by merging
            # Format and append to order history display from the dict
            history_entry = (
                f"[{QDateTime.currentDateTime().toString('HH:mm:ss')}] "
                f"ID: {final_order_data.get('order_id', order_id_key_str)}, " # Use 'order_id' from OrderData
                f"Symbol: {final_order_data.get('symbol', self.config.get('symbol'))}, "
                f"Action: {final_order_data.get('asset_type', '')} {final_order_data.get('instruction', '')}, "
                f"Qty: {final_order_data.get('quantity', 'N/A')}, "
                f"FilledQty: {final_order_data.get('filled_quantity', 'N/A')}, "
                f"AvgPx: {final_order_data.get('avg_fill_price', 'N/A')}, "
                f"Status: {final_order_data.get('status', 'N/A')}"
            )
            self.order_history_display.append(history_entry)
            self.order_history_display.ensureCursorVisible()
            self.refresh_active_orders_display() # Update active orders list too
        else: # If order wasn't in active_orders (e.g. direct update for a completed one not yet in self.active_orders)
             # This case becomes less likely if all updates go through handle_worker_order_update -> update_active_order_in_ui
            history_entry = (
                f"[{QDateTime.currentDateTime().toString('HH:mm:ss')}] "
                f"ID: {order_data_dict.get('order_id', order_id_key_str)}, "
                f"Symbol: {order_data_dict.get('symbol', self.config.get('symbol'))}, "
                f"Action: {order_data_dict.get('asset_type', '')} {order_data_dict.get('instruction', '')}, "
                f"Qty: {order_data_dict.get('quantity', 'N/A')}, "
                f"FilledQty: {order_data_dict.get('filled_quantity', 'N/A')}, "
                f"AvgPx: {order_data_dict.get('avg_fill_price', 'N/A')}, "
                f"Status: {order_data_dict.get('status', 'N/A')}"
            )
            self.order_history_display.append(history_entry)
            self.order_history_display.ensureCursorVisible()


    def refresh_active_orders_display(self):
        self.active_orders_display.clear()
        if not self.active_orders: # self.active_orders now holds dicts
            self.active_orders_display.setPlaceholderText("No active orders.")
            return

        for order_id_str, data_dict in self.active_orders.items():
            display_str = (
                f"ID: {order_id_str}, Symbol: {data_dict.get('symbol', self.config.get('symbol'))}, "
                f"Status: {data_dict.get('status', 'N/A')}, Qty: {data_dict.get('quantity', 'N/A')}, "
                f"Filled: {data_dict.get('filled_quantity', 'N/A')}, "
                f"Price: {data_dict.get('price', 'N/A') or data_dict.get('avg_fill_price', 'N/A')}"
            )
            self.active_orders_display.append(display_str)
        self.active_orders_display.ensureCursorVisible()


    def load_symbol_and_stream(self):
        if self.is_streaming:
            # This button might change to "Disconnect Stream"
            # self.disconnect_stream()
            # self.update_ui_element_states()
            # logger.info("Stream disconnected by user.")
            # self.statusBar().showMessage("Stream disconnected.")
            QMessageBox.information(self, "Stream Active", "Stream is already active. Disconnect first if you want to change symbol via this button.")
            return

        symbol = self.symbol_input.text().upper()
        if not Validator.is_valid_symbol(symbol):
            QMessageBox.warning(self, "Symbol Error", "Invalid symbol format.")
            logger.warning(f"Invalid symbol for streaming: {symbol}")
            return

        # Update config if symbol changed through here (and not through "Update Params")
        if self.config.get("symbol") != symbol:
            self.config["symbol"] = symbol
            self.save_config() # Save immediately
            logger.info(f"Symbol updated to {symbol} and saved via Load Symbol button.")

        self.current_symbol_display.setText(symbol)
        logger.info(f"Loading symbol: {symbol}")
        self.statusBar().showMessage(f"Loading {symbol}...")

        # Fetch initial account ID if not already set (example)
        if not self.account_id and self.client:
            try:
                # This is a blocking call, consider if it's okay here or needs a thread
                # acc_res = self.client.get_account_numbers() # Fictitious method
                # if acc_res.ok() and acc_res.json():
                # self.account_id = acc_res.json()[0].get('hashValue') # Example path
                # logger.info(f"Fetched account ID: {self.account_id}")
                # For now, assume account ID is known or handled by client/worker internally
                pass
            except Exception as e:
                logger.error(f"Failed to fetch account ID: {e}")
                # QMessageBox.critical(self, "Account Error", f"Failed to fetch account ID: {e}")
                # return # Stop if account ID is critical for streaming setup

        # Initialize StreamClient using the factory
        if not self.stream_client_factory:
            logger.error("Stream client factory not available. Cannot start stream.")
            QMessageBox.critical(self, "Stream Error", "Stream client factory is not configured.")
            self.statusBar().showMessage("Stream client factory missing.")
            return

        try:
            # Pass necessary parameters to the stream client factory
            # The factory should handle client authentication, token management etc.
            self.stream_client = self.stream_client_factory(
                client=self.client, # Pass the authenticated Schwab client
                account_id=self.account_id, # May or may not be needed by StreamClient itself
                on_data_received=self.handle_stream_data # Callback for data
            )
            logger.info("StreamClient instance created via factory.")
        except Exception as e:
            logger.error(f"Failed to create StreamClient via factory: {e}")
            QMessageBox.critical(self, "Stream Error", f"Failed to initialize stream client: {e}")
            self.statusBar().showMessage("Stream client initialization failed.")
            return

        # Define stream subscriptions
        # These service names might need to be exact as per Schwab API docs
        # LEVELONE_EQUITIES, ACCT_ACTIVITY are common
        # Note: The `Stream` class from `schwab.streaming` might have its own way to define these
        # This is a generic representation.
        # The provided Grok script uses:
        # request = {"requests": [
        # {"service": "ACCT_ACTIVITY", ...},
        # {"service": "LEVELONE_EQUITIES", ...}
        # ]}
        # This implies the StreamClient's subscribe method should handle this format.

        subscriptions = [
            {"service": "LEVELONE_EQUITIES", "keys": symbol, "fields": "0,1,2,3,4,5,8,9,10,11,12,13,14,15,16,17,18,24,25,27,28,29,30,31,49,50,51"}, # Example fields
            {"service": "ACCT_ACTIVITY", "keys": self.stream_client.user_principal_details.get('streamerSubscriptionKeys', {}).get('keys', [{}])[0].get('key'), "fields":"0,1,2,3"} # Key from user principals
        ]
        # The actual key for ACCT_ACTIVITY comes from user principals, StreamClient should handle this.
        # The factory or StreamClient itself might fetch user principals.

        if not self.stream_client: # Should have been created above
            logger.error("Stream client is not initialized. Cannot subscribe.")
            self.statusBar().showMessage("Stream client not ready for subscription.")
            return

        try:
            logger.info(f"Attempting to connect and subscribe stream for {symbol}...")
            self.statusBar().showMessage(f"Connecting stream for {symbol}...")

            # The StreamClient's subscribe method might be blocking or run in its own thread.
            # The original Grok script starts stream in a new thread.
            # Let's assume self.stream_client.subscribe() is non-blocking or we manage thread here.
            # For now, assume StreamClient's `subscribe` or `start` method handles threading internally
            # or is designed to be run in a separate thread.

            # Based on Grok script, stream is started in a thread:
            # self.stream_thread = threading.Thread(target=self.stream_client.start, args=(subscriptions,))
            # self.stream_thread.daemon = True
            # self.stream_thread.start()

            # Let's assume the stream client has a method like `start_streaming(subscriptions)`
            # which internally handles the connection and thread.
            # Or, if `subscribe` starts it:

            # The `Stream` class from `schwab.streaming` likely has a method to start
            # For example, it might be `self.stream_client.start_stream(requests_data)`
            # The Grok `Stream` class has `stream.start(request)`

            # This is a critical part that depends on the exact StreamClient implementation.
            # For now, let's assume a method `start_stream_async(subscriptions)` exists
            # that handles threading. If not, we'll need to wrap the call in a Thread.

            # Replicating Grok's threading model for the stream:
            if hasattr(self.stream_client, 'build_requests_and_subscribe'): # adapting to a hypothetical method
                self.stream_thread = Thread(target=self.stream_client.build_requests_and_subscribe, args=(subscriptions,))
                self.stream_thread.daemon = True
                self.stream_thread.start()
                self.is_streaming = True
                logger.info(f"Stream connection process started for {symbol} in a new thread.")
                self.statusBar().showMessage(f"Streaming {symbol}...")
            elif hasattr(self.stream_client, 'start'): # Generic start method, like in Grok
                 # The `start` method in Grok's Stream takes a request dictionary.
                 # We need to format `subscriptions` into that request.
                stream_request_payload = self.stream_client.build_level_one_equity_stream_request([symbol]) # Example
                # Add account activity if needed - Grok's Stream class seems to handle this internally or via user principals
                # For now, let's assume the StreamClient can take the list of subscriptions directly,
                # or has a method to build the full request.
                # This part needs to align with the actual StreamClient's API.
                # Let's assume the factory configured the stream client correctly and it knows what to do.
                # For the sake of progress, assume a generic start method that takes subscriptions:

                # This is the most problematic part due to unknown StreamClient API.
                # Let's assume the StreamClient was given the callback and will use it.
                # And its `subscribe` method (if it exists) or a `start` method initiates.
                # The `schwab.streaming.Stream` class is likely the one from the SDK.
                # It would typically have a `start()` and `subscribe()` methods.

                # Based on typical SDK patterns, you subscribe first, then start.
                # Or `start` takes subscriptions.
                # The Grok `Stream` class has `stream.start(request_data)` where request_data is the full JSON.

                # Let's try to mimic the Grok structure assuming self.stream_client is that Stream class.
                # The Stream class in Grok's example seems to manage its own thread after start() is called.

                # The following is a placeholder for the actual stream starting logic
                # This will likely need to be:
                # 1. Build the request object (as per Schwab API for streaming)
                # 2. Call a method like self.stream_client.start(request_object)
                # This request object usually includes services, keys, parameters, fields.

                # For now, assuming the Worker or StreamClient handles the actual subscription details
                # and this call just "activates" it for the given symbol.
                # This is a simplification.

                # If self.stream_client is an instance of `schwab.streaming.Stream`
                # it needs to be started, and then subscriptions added.
                # This is a complex part that needs the exact API of `schwab.streaming.Stream`.
                # For now, let's assume the `worker` handles stream setup if it can.
                if self.worker and hasattr(self.worker, 'start_data_stream'):
                    self.worker.start_data_stream(symbol, subscriptions) # Worker handles stream thread
                    self.is_streaming = True # Assume worker sets this or signals success
                    logger.info(f"Handed off stream start for {symbol} to worker.")
                else:
                    # Fallback: Try a generic start if worker doesn't handle it
                    # This is highly speculative without StreamClient API details
                    logger.warning("Worker does not have 'start_data_stream'. Attempting basic stream start (speculative).")
                    # This part is very likely to fail or be incorrect.
                    # self.stream_client.some_generic_start_method(subscriptions)
                    # For now, we cannot reliably start the stream here without more info.
                    # Let's log an error and not set is_streaming = True
                    err_msg = "Cannot start stream: No clear method in Worker and StreamClient API is unknown."
                    logger.error(err_msg)
                    QMessageBox.critical(self, "Stream Error", err_msg)
                    self.statusBar().showMessage("Stream start failed (config error).")
                    self.current_symbol_display.setText("N/A") # Revert
                    self.is_streaming = False


            else:
                logger.error("StreamClient does not have a recognized method to start streaming (e.g., 'build_requests_and_subscribe' or 'start').")
                QMessageBox.critical(self, "Stream Error", "Cannot start stream: Client interface incompatible.")
                self.statusBar().showMessage("Stream start failed (client error).")
                self.is_streaming = False
                self.current_symbol_display.setText("N/A") # Revert symbol display


        except Exception as e:
            self.is_streaming = False
            logger.error(f"Failed to start or subscribe to stream for {symbol}: {e}", exc_info=True)
            QMessageBox.critical(self, "Stream Error", f"Could not start stream for {symbol}: {e}")
            self.statusBar().showMessage(f"Stream failed for {symbol}.")
            self.current_symbol_display.setText("N/A") # Revert symbol display

        self.update_ui_element_states()


    def disconnect_stream(self):
        logger.info("Attempting to disconnect stream...")
        self.statusBar().showMessage("Disconnecting stream...")
        if self.stream_client and hasattr(self.stream_client, 'stop'): # Or 'close', 'disconnect'
            try:
                self.stream_client.stop() # Assuming this method exists and cleans up
                logger.info("Stream stop command issued.")
            except Exception as e:
                logger.error(f"Error during stream stop: {e}", exc_info=True)

        if self.stream_thread and self.stream_thread.is_alive():
            try:
                # Stream client should ideally handle thread termination gracefully
                # Forcibly joining can hang if thread is stuck.
                # self.stream_thread.join(timeout=5.0) # Wait for thread to finish
                logger.info("Stream thread joining attempt (if applicable).")
                # If stream_client.stop() signals the thread, join might work.
                # Otherwise, if the thread runs a loop, it needs an internal flag.
            except Exception as e:
                logger.error(f"Error joining stream thread: {e}")

        self.is_streaming = False
        self.stream_client = None # Release the client
        self.stream_thread = None

        # Clear market data displays
        self.current_symbol_display.setText("N/A")
        self.bid_price_label.setText("N/A")
        self.ask_price_label.setText("N/A")
        self.last_price_label.setText("N/A")

        logger.info("Stream disconnected and resources released (attempted).")
        self.statusBar().showMessage("Stream disconnected.")
        self.update_ui_element_states()


    def toggle_trading(self):
        self.trading_enabled = self.enable_trading_button.isChecked()
        if self.trading_enabled:
            self.enable_trading_button.setText("Trading ENABLED (Click to Disable)")
            self.enable_trading_button.setStyleSheet("background-color: lightgreen;")
            logger.info("Trading has been ENABLED by user.")
            self.statusBar().showMessage("Trading ENABLED.")
            # Potentially trigger worker to start its trading logic if it was paused
            if self.worker and hasattr(self.worker, 'set_trading_active'):
                self.worker.set_trading_active(True)
        else:
            self.enable_trading_button.setText("Trading DISABLED (Click to Enable)")
            self.enable_trading_button.setStyleSheet("") # Reset style
            logger.info("Trading has been DISABLED by user.")
            self.statusBar().showMessage("Trading DISABLED.")
            if self.worker and hasattr(self.worker, 'set_trading_active'):
                self.worker.set_trading_active(False)
                # Also, potentially cancel pending strategy orders if that's desired
                # self.worker.cancel_strategy_orders() # Example
        self.update_ui_element_states()

    def update_manual_price_input_state(self, order_type_text: str):
        """Enables or disables the manual price input based on order type."""
        is_limit = order_type_text == "LIMIT"
        self.manual_price_input.setEnabled(is_limit)
        if not is_limit:
            self.manual_price_input.setText("0.00") # Or clear it: self.manual_price_input.clear()
        # Ensure overall enabled state is respected if called outside update_ui_element_states context
        is_trading_allowed = self.enable_trading_button.isChecked()
        is_streaming_active = self.is_streaming
        self.manual_price_input.setEnabled(is_limit and is_streaming_active and is_trading_allowed)


    def manual_buy_at_market(self): # Renamed from manual_buy
        if not self.trading_enabled:
            QMessageBox.warning(self, "Trading Disabled", "Please enable trading first.")
            return
        if not self.worker:
            QMessageBox.critical(self, "Worker Error", "Trading worker is not available.")
            logger.error("Manual buy market attempted but worker is not initialized.")
            return
        if not hasattr(self.worker, 'on_manual_buy_requested'): # Check if worker has the new method
            QMessageBox.critical(self, "Worker Error", "Worker does not support 'on_manual_buy_requested'.")
            return

        symbol = self.config.get("symbol")
        quantity = self.config.get("quantity")
        # For manual_buy_at_market, order_type is MARKET, price is not strictly needed by worker but can be indicative
        price = self.current_ask # Indicative price
        order_type = "MARKET"

        reply = QMessageBox.question(self, "Confirm Manual Buy Market",
                                     f"Place a MARKET BUY order for {quantity} {symbol} (indicative price: {price})?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            logger.info(f"User initiated MANUAL BUY MARKET: {quantity} {symbol} at current ask {price}")
            self.worker.on_manual_buy_requested(symbol=symbol, quantity=quantity, order_type=order_type, price=None) # Price is None for Market
            # Status bar message will be updated via worker signals
        else:
            logger.info("Manual buy market cancelled by user.")
            self.statusBar().showMessage("Manual buy market cancelled.")


    def manual_sell_all_at_market(self): # Renamed from manual_sell_all
        if not self.trading_enabled:
            QMessageBox.warning(self, "Trading Disabled", "Please enable trading first.")
            return
        if not self.worker:
            QMessageBox.critical(self, "Worker Error", "Trading worker is not available.")
            logger.error("Manual sell market attempted but worker is not initialized.")
            return
        if not hasattr(self.worker, 'on_manual_sell_requested'): # Check if worker has the new method
            QMessageBox.critical(self, "Worker Error", "Worker does not support 'on_manual_sell_requested'.")
            return

        symbol = self.config.get("symbol")
        current_position_qty = self.worker.get_current_holdings(symbol) if hasattr(self.worker, 'get_current_holdings') else 0

        if current_position_qty <= 0:
            QMessageBox.information(self, "No Position", f"No current position to sell for {symbol}.")
            logger.info(f"Manual sell all market for {symbol} attempted, but no position reported by worker.")
            return

        price = self.current_bid # Indicative price
        order_type = "MARKET"

        reply = QMessageBox.question(self, "Confirm Manual Sell All Market",
                                     f"Place a MARKET SELL order for {current_position_qty} shares of {symbol} (indicative price: {price})?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            logger.info(f"User initiated MANUAL SELL ALL MARKET: {current_position_qty} {symbol} at current bid {price}")
            self.worker.on_manual_sell_requested(symbol=symbol, quantity=current_position_qty, order_type=order_type, price=None) # Price is None for Market
            # Status bar message will be updated via worker signals
        else:
            logger.info("Manual sell all market cancelled by user.")
            self.statusBar().showMessage("Manual sell all market cancelled.")


    def handle_generic_manual_buy(self):
        """Handles the 'Place Manual Buy' button click."""
        if not self.trading_enabled:
            QMessageBox.warning(self, "Trading Disabled", "Please enable trading first.")
            return
        if not self.worker or not hasattr(self.worker, 'on_manual_buy_requested'):
            QMessageBox.critical(self, "Worker Error", "Trading worker is not available or does not support this action.")
            return

        symbol = self.symbol_input.text().upper() # Use current symbol from main input
        try:
            quantity = int(self.quantity_input.text()) # Use current quantity from main input
            if not Validator.is_valid_quantity(quantity):
                raise ValueError("Invalid quantity")
        except ValueError:
            QMessageBox.warning(self, "Input Error", "Invalid quantity provided.")
            return

        order_type = self.manual_order_type_combo.currentText()
        price_str = self.manual_price_input.text()
        price: Optional[float] = None

        if not Validator.is_valid_symbol(symbol):
            QMessageBox.warning(self, "Input Error", "Invalid symbol.")
            return

        if order_type == "LIMIT":
            if not Validator.is_valid_price(price_str):
                QMessageBox.warning(self, "Input Error", "Invalid price for LIMIT order.")
                return
            price = float(price_str)

        log_msg = (f"Attempting GENERIC MANUAL BUY: {order_type} {quantity} {symbol} "
                   f"{('@ ' + str(price)) if price is not None else ''}")
        logger.info(log_msg)
        self.append_log_message_to_ui(log_msg) # Also log to UI directly

        # Call worker's method
        self.worker.on_manual_buy_requested(symbol=symbol, quantity=quantity, order_type=order_type, price=price)


    def handle_generic_manual_sell(self):
        """Handles the 'Place Manual Sell' button click."""
        if not self.trading_enabled:
            QMessageBox.warning(self, "Trading Disabled", "Please enable trading first.")
            return
        if not self.worker or not hasattr(self.worker, 'on_manual_sell_requested'):
            QMessageBox.critical(self, "Worker Error", "Trading worker is not available or does not support this action.")
            return

        symbol = self.symbol_input.text().upper()
        try:
            quantity = int(self.quantity_input.text())
            if not Validator.is_valid_quantity(quantity):
                raise ValueError("Invalid quantity")
        except ValueError:
            QMessageBox.warning(self, "Input Error", "Invalid quantity provided.")
            return

        order_type = self.manual_order_type_combo.currentText()
        price_str = self.manual_price_input.text()
        price: Optional[float] = None

        if not Validator.is_valid_symbol(symbol):
            QMessageBox.warning(self, "Input Error", "Invalid symbol.")
            return

        # Check holdings for sell, unless it's a short sell (not explicitly supported here)
        # current_holdings = self.worker.get_current_holdings(symbol) if hasattr(self.worker, 'get_current_holdings') else 0
        # if quantity > current_holdings:
        #     QMessageBox.warning(self, "Sell Error", f"Cannot sell {quantity}; holdings: {current_holdings} (real check needed).")
        #     return

        if order_type == "LIMIT":
            if not Validator.is_valid_price(price_str):
                QMessageBox.warning(self, "Input Error", "Invalid price for LIMIT order.")
                return
            price = float(price_str)

        log_msg = (f"Attempting GENERIC MANUAL SELL: {order_type} {quantity} {symbol} "
                   f"{('@ ' + str(price)) if price is not None else ''}")
        logger.info(log_msg)
        self.append_log_message_to_ui(log_msg)

        # Call worker's method
        self.worker.on_manual_sell_requested(symbol=symbol, quantity=quantity, order_type=order_type, price=price)


    def cancel_all_active_orders(self):
        if not self.worker:
            QMessageBox.critical(self, "Worker Error", "Trading worker is not available to cancel orders.")
            logger.error("Cancel all orders attempted but worker is not initialized.")
            return
        if not hasattr(self.worker, 'on_cancel_all_orders_requested'):
            QMessageBox.critical(self, "Worker Error", "Worker does not support 'on_cancel_all_orders_requested'.")
            return

        symbol_to_cancel = self.config.get("symbol") # Cancel orders for the current symbol in parameters
        # Or offer to cancel all regardless of symbol: symbol_to_cancel = None

        reply = QMessageBox.question(self, "Confirm Cancel All",
                                     f"Cancel ALL active orders for symbol {symbol_to_cancel}?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            logger.info(f"User initiated CANCEL ALL ORDERS for symbol {symbol_to_cancel}.")
            self.worker.on_cancel_all_orders_requested(symbol_to_cancel=symbol_to_cancel)
            # Status messages handled by worker signals
        else:
            logger.info("Cancel all orders aborted by user.")
            self.statusBar().showMessage("Cancel all orders aborted.")


    def closeEvent(self, event):
        """Handle window close event."""
        logger.info("Close event triggered. Application shutting down.")
        self.statusBar().showMessage("Shutting down...")

        # Save notepad and config one last time
        self.save_notepad()
        # self.save_config() # Config should be saved on update, but maybe save again if params changed without "Update"

        # Disconnect stream if active
        if self.is_streaming:
            logger.info("Closing: Disconnecting active stream.")
            self.disconnect_stream() # This should also try to join the thread

        # Signal worker to stop (if applicable)
        if self.worker and hasattr(self.worker, 'cleanup_and_exit'): # Changed to new method name
            logger.info("Closing: Signaling worker to cleanup and exit.")
            self.worker.cleanup_and_exit()
        elif self.worker and hasattr(self.worker, 'stop'): # Fallback if old name still there
             logger.info("Closing: Signaling worker to stop (fallback).")
             self.worker.stop()


        # Stop UI timer
        if self.ui_update_timer.isActive():
            self.ui_update_timer.stop()

        # Perform any other cleanup
        logger.info("TradingDashboard shutdown procedures complete.")
        # print("TradingDashboard shutdown complete.") # For console debugging if logger isn't visible
        QApplication.instance().quit() # Ensure application quits
        event.accept()


# --- Main Application Execution ---
def main():
    # For profiling or tracing (optional)
    # tracemalloc.start() # For memory allocation tracing
    # profiler = cProfile.Profile() # For performance profiling
    # profiler.enable()

    # Load environment variables first
    # This is global for now, consider passing as args or into a context object
    try:
        load_environment_variables()
        # Check if critical variables were loaded
        if not app_key_global or not app_secret_global or not callback_url_global:
            # Message already logged by load_environment_variables
            # A GUI app might show this error more gracefully before even trying to start PyQt
            print("Critical environment variables missing. Exiting application.", file=sys.stderr)
            # If QMessageBox was used in load_environment_variables and it's pre-QApplication, it won't show.
            # So, ensure console output for this critical startup failure.
            # For a real GUI app, this check should happen, and if fails, show a simple error dialog
            # without initializing the full app, or a pre-init splash screen/dialog.
            # For now, we'll proceed and let client init fail if they are missing,
            # but ideally, exit here if they are not set.
            # Let's assume for now the app can start and show the error later if client fails.
            pass # Allow app to proceed and potentially fail at client creation

    except EnvironmentError as e: # If load_environment_variables raises it
        # This error should be shown in a way the user can see before full GUI load.
        # For simplicity, printing to stderr. A real app needs a pre-GUI error dialog.
        print(f"Configuration Error: {e}", file=sys.stderr)
        # We could try to show a QMessageBox here if QApplication can be initialized minimally.
        # temp_app_for_error_dialog = QApplication.instance() or QApplication(sys.argv)
        # QMessageBox.critical(None, "Configuration Error", str(e))
        # sys.exit(1)
        # For now, let the app try to start; client init will fail.
        pass


    # Setup logging (after env vars, in case log path comes from env)
    # Ensure log directory exists, or logging setup might fail.
    # LOG_FILENAME might include a path like 'logs/trading_app.log'
    log_dir = os.path.dirname(LOG_FILENAME)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir, exist_ok=True)
        except OSError as e:
            print(f"Error creating log directory {log_dir}: {e}. Log file will be in current directory.", file=sys.stderr)
            # Fallback to just the filename in the current directory
            # LOG_FILENAME = os.path.basename(LOG_FILENAME) # This would modify global; better to pass to setup_logging

    setup_logging(LOG_FILENAME, MAX_LOG_FILE_SIZE_MB, LOG_BACKUP_COUNT, log_queue) # log_queue is global

    logger.info("Application starting...")
    # print("Application starting... (console check)") # For debugging before logger is fully confirmed

    app = QApplication(sys.argv)

    # --- Schwab Client and Streamer Initialization ---
    # This is where the actual Schwab client and stream client factory would be set up.
    # For now, these are placeholders. They will be replaced by actual implementations.

    # Placeholder for Schwab Client
    # The actual client needs APP_KEY, APP_SECRET, CALLBACK_URL, TOKEN_FILE_PATH
    # These should be available globally or passed to a factory function.

    schwab_client = None
    try:
        if app_key_global and app_secret_global and callback_url_global:
            logger.info(f"Initializing Schwab Client with Key: {app_key_global[:4]}..., Token Path: {token_file_path_global}")
            # This is where you'd instantiate schwab.client.Client
            # from schwab.client import Client # Assuming this is the correct import
            schwab_client = Client(
                app_key=app_key_global,
                app_secret=app_secret_global,
                callback_url=callback_url_global,
                token_path=token_file_path_global # Ensure your Client class takes this
            )
            # Perform authentication if necessary (e.g., client.login())
            # This might involve a browser flow if token is not present/valid.
            # For a GUI app, this needs careful handling (e.g., dialog for auth).
            logger.info("Schwab Client object created (pending authentication/token validation).")
            # Example: Check token or trigger auth flow (simplified)
            # if not schwab_client.is_token_valid(): # Hypothetical method
            #     logger.info("Token is invalid or missing. Authentication may be required.")
            #     # schwab_client.manual_auth_flow() or similar
            # else:
            #     logger.info("Token found and appears valid.")

        else:
            logger.error("Schwab Client cannot be initialized: Missing API credentials.")
            QMessageBox.critical(None, "API Credentials Error",
                                 "Schwab API Key, Secret, or Callback URL not configured. Please set environment variables.")
            # sys.exit(1) # Exit if client cannot be created. Or let UI load in a disabled state.
            # For now, let UI load but many things will be broken.
    except Exception as e:
        logger.error(f"Failed to initialize Schwab Client: {e}", exc_info=True)
        QMessageBox.critical(None, "Client Initialization Error", f"Could not initialize Schwab Client: {e}")
        # sys.exit(1)
        # Let UI load to show logs.


    # Placeholder for Stream Client Factory
    # This factory would create instances of your StreamClient (e.g., schwab.streaming.Stream)
    def stream_client_factory_placeholder(client, account_id, on_data_received):
        logger.info(f"Stream client factory called. Account ID: {account_id}")
        if not client:
            logger.error("Stream client factory: Schwab client is None.")
            raise ValueError("Schwab client not provided to stream factory.")

        # This is where you'd instantiate your actual stream client, e.g.,
        # from schwab.streaming import Stream # Assuming this is the one
        # stream_instance = Stream(schwab_client_instance=client, ...)
        # stream_instance.set_data_handler(on_data_received) # Or similar callback mechanism

        # For now, returning a mock/dummy stream client object if schwab.streaming.Stream is not fully defined
        # or if it requires complex setup not yet available.
        try:
            # Attempt to use the imported schwab.streaming.Stream
            # The actual Stream class from schwab-py might need more setup (e.g. streamer_info)
            # For this placeholder, we assume it can be instantiated simply or is already configured by client.

            # The Grok script's Stream class takes: client, account_id, q, error_q, control_q
            # Our `on_data_received` is like `q`.
            # This implies that `schwab.streaming.Stream` might be a custom class from Grok,
            # not directly the one from `schwab-py` SDK without adaptation.

            # Let's assume `schwab.streaming.Stream` is the SDK's stream client.
            # It usually requires the main `Client` object.
            # streamer = Stream(client=client, data_callback=on_data_received) # Hypothetical SDK Stream

            # For the purpose of this step, let's assume the Stream class from the prompt is
            # compatible with this instantiation or that `schwab.streaming.Stream` is such a class.
            # This part will likely need refinement once the true nature of `schwab.streaming.Stream` is clear.

            # If `Stream` is the one from Grok's code, it would need queues.
            # Let's assume a simplified constructor for now for the SDK's Stream, or that the Worker will handle it.

            # For now, we'll pass a reference to the class `Stream` itself if it's just a type hint,
            # or try to instantiate if it's meant to be functional here.
            # The TradingDashboard expects a factory that *returns* an instance.

            # This is the most uncertain part due to the `Stream` class's origin.
            # If `schwab.streaming.Stream` is the SDK's, it's often used like:
            # stream_client = client.get_streamer_client() # or similar
            # stream_client.add_data_listener(on_data_received)
            # For now, let's assume a simple constructor for `Stream` for placeholder purposes.

            # This is a placeholder factory. The actual implementation will depend on how
            # the schwab.streaming.Stream class is meant to be used.
            # If it's the Grok Stream, it needs queues. If SDK, it needs different setup.
            # For now, let's simulate that it returns a new instance of the imported Stream class.

            # This will likely fail if `Stream` requires more arguments or complex setup.
            # We are just fulfilling the factory contract.
            # stream_instance = Stream(schwab_client_instance=client, data_handler_func=on_data_received) # Made up signature

            # Given the prompt implies `schwab.streaming.Stream` is available,
            # let's assume it's a class that can be instantiated.
            # The actual arguments are unknown. This is a placeholder.
            # It's possible the worker should be creating this, not the factory directly.

            # Let's assume the factory provides an object that has methods like `start` and `stop`.
            # The `schwab-api` library's `StreamClient` (if that's what `Stream` refers to)
            # is typically obtained from `client.create_streaming_client()`.

            if hasattr(client, 'create_streaming_client'): # Check for schwab-api like method
                 sdk_stream_client = client.create_streaming_client()
                 # We need to adapt this sdk_stream_client to the interface expected by TradingDashboard,
                 # especially the `start(subscriptions)` and `stop()` methods, and the callback.
                 # This might involve wrapping it. For now, let's log.
                 logger.info("Using SDK's create_streaming_client(). Further adaptation might be needed.")
                 # This is not a full factory yet, just shows how one might get the client.
                 # The TradingDashboard expects the factory to return a ready-to-use object.
                 # For now, this factory will be passed but `load_symbol_and_stream` will fail
                 # if the returned object isn't what it expects.
                 # This is a known gap.
                 # For now, let's return a dummy object that won't work but allows UI to load.
                 class DummyStreamClient:
                    def __init__(self, cl, cb):
                        self.cl = cl
                        self.cb = cb
                        self.user_principal_details = {} # Placeholder
                        logger.info("DummyStreamClient created.")
                    def build_requests_and_subscribe(self, subs): # Method load_symbol_and_stream tries
                        logger.info(f"DummyStreamClient: build_requests_and_subscribe called with {subs}")
                        # Simulate async operation / thread
                        def _dummy_stream():
                            time.sleep(2)
                            logger.info("DummyStreamClient: Simulating data...")
                            dummy_data = [{'service': 'QUOTE', 'content': [{'key': 'DUMMY', 'BID_PRICE': 10.0, 'ASK_PRICE': 10.5, 'LAST_PRICE': 10.25}]}]
                            if self.cb: self.cb(dummy_data)
                        # Thread(target=_dummy_stream).start() # Example of running it
                        logger.warning("DummyStreamClient does not actually stream real data.")
                    def stop(self): logger.info("DummyStreamClient: stop() called.")
                 return DummyStreamClient(client, on_data_received)

            else:
                logger.warning("Schwab client does not have 'create_streaming_client()'. Stream functionality will be limited/mocked.")
                # Fallback to a more generic dummy if the above isn't possible.
                class GenericDummyStream:
                    def __init__(self, *args, **kwargs): self.user_principal_details = {} # Placeholder
                    def start(self, *args, **kwargs): logger.info("GenericDummyStream.start called")
                    def stop(self, *args, **kwargs): logger.info("GenericDummyStream.stop called")
                    def build_requests_and_subscribe(self, *args, **kwargs): logger.info("GenericDummyStream.build_requests_and_subscribe called")

                return GenericDummyStream()


        except Exception as e_stream_factory:
            logger.error(f"Error in stream_client_factory_placeholder: {e_stream_factory}", exc_info=True)
            # Return a non-functional mock to allow UI to load
            class ErrorStreamClient:
                def __init__(self, *args, **kwargs): logger.error("ErrorStreamClient instantiated due to factory failure.")
                def start(self, *args, **kwargs): logger.error("ErrorStreamClient: start called, but non-functional.")
                def stop(self, *args, **kwargs): logger.error("ErrorStreamClient: stop called.")
                def build_requests_and_subscribe(self, *args, **kwargs): logger.error("ErrorStreamClient: build_requests_and_subscribe called, non-functional.")
                @property
                def user_principal_details(self): return {} # Placeholder property
            return ErrorStreamClient()

    # Placeholder for Worker Factory
    # This factory would create instances of your Worker class.
    def worker_factory_placeholder(client, dashboard_ref):
        logger.info("Worker factory called.")
        # This is where you'd instantiate your Worker class from trading_logic.py
        # from schwab.trading_logic import Worker # This will be done later

        # Now instantiating the placeholder Worker defined in this file.
        # This will be changed later when Worker is moved to trading_logic.py
        if 'Worker' in globals():
             logger.info("Placeholder Worker class found in globals. Attempting to use it.")
             return Worker(client, dashboard_ref) # type: ignore
        else:
            # Fallback to a very basic dummy if Worker class isn't found (should not happen)
            logger.error("Worker class not found in globals. Worker functionality will be severely limited.")
            class EmergencyDummyWorker:
                def __init__(self, cl, dash): logger.info("EmergencyDummyWorker created.")
                def __getattr__(self, name): return lambda *args, **kwargs: logger.error(f"EmergencyDummyWorker: {name} called, but non-functional.")
            return EmergencyDummyWorker(client, dashboard_ref)


    # Create and show the main window
    main_window = TradingDashboard(
        client=schwab_client,
        stream_client_factory=stream_client_factory_placeholder,
        worker_factory=worker_factory_placeholder
    )
    main_window.show()

    exit_code = app.exec()

    # Profiling end (optional)
    # profiler.disable()
    # profiler.dump_stats("profile.prof")
    # current, peak = tracemalloc.get_traced_memory()
    # logger.info(f"Current memory usage: {current / 10**6}MB; Peak: {peak / 10**6}MB")
    # tracemalloc.stop()

    logger.info(f"Application exiting with code {exit_code}.")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
