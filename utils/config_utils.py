"""
Utility functions for managing application configuration stored in a JSON file (`config.json`).

Provides functionality to:
- Load configuration, creating a default file if one doesn't exist or is invalid.
- Ensure the configuration contains all necessary keys, updating from defaults if needed.
- Save configuration changes back to the file.
- Get and set specific configuration values, including nested values.
- Helper functions for commonly accessed settings like database path and report paths.
"""
import json
import os
import streamlit as st

CONFIG_FILE = "config.json"
DEFAULT_CONFIG = {
    "database_path": "data/marketplace_data.db",
    # Database mode: "local" (file path) or "motherduck" (cloud)
    "database_mode": "local",
    # MotherDuck-specific settings
    "motherduck": {
        "db_name": "",   # e.g., "datafox_sl" or "workspace/datafox_sl"
        "token": ""       # Optional, can also come from env MOTHERDUCK_TOKEN
    },
    "report_paths": {
        "oz_barcodes_xlsx": "",
        "oz_orders_csv": "",
        "oz_prices_xlsx": "",
        "oz_products_csv": "",
        "wb_prices_xlsx": "",
        "wb_products_dir": "",
        "oz_card_rating_xlsx": "",
        "analytic_report_xlsx": ""
    },
    "data_filters": {
        "oz_category_products_brands": ""
    },
    "margin_calculation": {
        "commission_percent": 36.0,
        "acquiring_percent": 0.0,
        "advertising_percent": 3.0,
        "vat_percent": 20.0,
        "exchange_rate": 90.0
    }
}

def load_config() -> dict:
    """
    Loads the configuration from config.json.
    If the file doesn't exist or is invalid, it creates/repairs it with default values,
    ensuring all keys from DEFAULT_CONFIG are present.

    Returns:
        dict: The configuration dictionary.
    """
    if not os.path.exists(CONFIG_FILE):
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()
    try:
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
            # Ensure all keys from default_config are present in the loaded config.
            # This handles cases where the config file might be outdated or missing some keys
            # after an application update.
            updated_config = False # Flag to track if config was modified
            for key, default_value in DEFAULT_CONFIG.items():
                if key not in config:
                    config[key] = default_value
                    updated_config = True
                elif isinstance(default_value, dict): # Check for nested dictionaries (e.g., report_paths)
                    if key not in config or not isinstance(config[key], dict):
                        config[key] = default_value # If key exists but is not a dict, overwrite with default dict
                        updated_config = True
                    else:
                        for sub_key, default_sub_value in default_value.items():
                            if sub_key not in config[key]:
                                config[key][sub_key] = default_sub_value
                                updated_config = True
            
            if updated_config:
                save_config(config) # Save if we added missing keys or structures
            return config
    except json.JSONDecodeError:
        st.error(f"Error decoding {CONFIG_FILE}. Reverting to default configuration.")
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()
    except Exception as e:
        st.error(f"An unexpected error occurred while loading {CONFIG_FILE}: {e}. Reverting to default configuration.")
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()

def save_config(config: dict) -> None:
    """
    Saves the given configuration dictionary to config.json.

    Args:
        config (dict): The configuration dictionary to save.
    """
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
    except Exception as e:
        st.error(f"Error saving configuration to {CONFIG_FILE}: {e}")

def get_config_value(key: str, sub_key: str = None, default=None):
    """
    Retrieves a specific value from the configuration.
    If the key or sub_key is not found, returns the provided default value.

    Args:
        key (str): The main configuration key.
        sub_key (str, optional): The sub-key if the value is nested (e.g., within 'report_paths').
                                Defaults to None.
        default: The value to return if the key/sub_key is not found. Defaults to None.

    Returns:
        Any: The configuration value or the default.
    """
    config = load_config()
    if sub_key:
        return config.get(key, {}).get(sub_key, default)
    return config.get(key, default)

def update_config_value(key: str, value, sub_key: str = None) -> None:
    """
    Updates a specific value in the configuration and saves the entire configuration.

    Args:
        key (str): The main configuration key.
        value: The new value to set.
        sub_key (str, optional): The sub-key if the value is nested. Defaults to None.
    """
    config = load_config()
    if sub_key:
        if key not in config or not isinstance(config[key], dict):
            config[key] = {}
        config[key][sub_key] = value
    else:
        config[key] = value
    save_config(config)

# --- Helper functions for specific config values ---

def get_db_path() -> str:
    """Returns the configured database path, falling back to the default if not set."""
    return get_config_value("database_path", default=DEFAULT_CONFIG["database_path"])

def set_db_path(path: str) -> None:
    """Sets and saves the database path in the configuration."""
    update_config_value("database_path", path)

def get_db_mode() -> str:
    """Returns current database mode: 'local' or 'motherduck'."""
    mode = get_config_value("database_mode", default=DEFAULT_CONFIG["database_mode"]) or "local"
    return mode if mode in ("local", "motherduck") else "local"

def set_db_mode(mode: str) -> None:
    """Sets database mode to 'local' or 'motherduck'."""
    normalized = mode.lower().strip()
    if normalized not in ("local", "motherduck"):
        normalized = "local"
    update_config_value("database_mode", normalized)

def get_motherduck_db_name() -> str:
    """Returns MotherDuck database name from config (may be empty)."""
    return get_config_value("motherduck", sub_key="db_name", default="")

def set_motherduck_db_name(name: str) -> None:
    """Sets MotherDuck database name in config."""
    update_config_value("motherduck", (name or "").strip(), sub_key="db_name")

def get_motherduck_token() -> str:
    """Returns MotherDuck token from config (may be empty)."""
    return get_config_value("motherduck", sub_key="token", default="")

def set_motherduck_token(token: str) -> None:
    """Sets MotherDuck token in config."""
    update_config_value("motherduck", token or "", sub_key="token")

def get_report_path(report_key: str) -> str:
    """
    Returns the configured path for a specific report key (e.g., 'oz_orders_csv').
    Report key should match one of the keys within DEFAULT_CONFIG['report_paths'].
    Returns an empty string if the report_key is not found or path is not set.
    """
    return get_config_value("report_paths", sub_key=report_key, default="")

def set_report_path(report_key: str, path: str) -> None:
    """
    Sets and saves the path for a specific report key in the configuration.
    Report key should match one of the keys within DEFAULT_CONFIG['report_paths'].
    """
    update_config_value("report_paths", path, sub_key=report_key)

def get_data_filter(filter_key: str) -> str:
    """
    Returns the configured filter value for a specific filter key (e.g., 'oz_category_products_brands').
    Returns an empty string if the filter_key is not found or filter is not set.
    """
    return get_config_value("data_filters", sub_key=filter_key, default="")

def set_data_filter(filter_key: str, filter_value: str) -> None:
    """
    Sets and saves the filter value for a specific filter key in the configuration.
    For example: filter_key='oz_category_products_brands', filter_value='Shuzzi;Nike;Adidas'
    """
    update_config_value("data_filters", filter_value, sub_key=filter_key)

# --- Helper functions for margin calculation configuration ---

def get_margin_config() -> dict:
    """
    Returns the complete margin calculation configuration dictionary.
    Enhanced with validation and error handling.
    If margin_calculation section doesn't exist, returns default values.
    
    Returns:
        dict: Dictionary containing all margin calculation parameters.
    """
    try:
        config = get_config_value("margin_calculation", default=DEFAULT_CONFIG["margin_calculation"])
        
        # Validate that config is a dictionary
        if not isinstance(config, dict):
            print(f"DEBUG: margin_calculation config is not a dict: {type(config)}")
            return DEFAULT_CONFIG["margin_calculation"].copy()
        
        # Ensure all required keys exist with valid values
        validated_config = {}
        default_margin_config = DEFAULT_CONFIG["margin_calculation"]
        
        for key, default_value in default_margin_config.items():
            try:
                value = config.get(key, default_value)
                
                # Validate value is numeric
                if value is None:
                    validated_config[key] = default_value
                elif isinstance(value, (int, float)):
                    validated_config[key] = float(value)
                else:
                    # Try to convert to float
                    validated_config[key] = float(value)
                    
            except (ValueError, TypeError):
                print(f"DEBUG: Invalid margin config value for {key}: {config.get(key)}, using default {default_value}")
                validated_config[key] = default_value
        
        return validated_config
        
    except Exception as e:
        print(f"DEBUG: Error loading margin config: {e}")
        return DEFAULT_CONFIG["margin_calculation"].copy()

def set_margin_config(config: dict) -> None:
    """
    Sets and saves the complete margin calculation configuration.
    Enhanced with validation and error handling.
    
    Args:
        config (dict): Dictionary containing margin calculation parameters.
                      Should include: commission_percent, acquiring_percent, 
                      advertising_percent, vat_percent, exchange_rate
    """
    try:
        if not isinstance(config, dict):
            raise ValueError(f"Config must be a dictionary, got {type(config)}")
        
        # Validate and sanitize the configuration
        validated_config = {}
        default_margin_config = DEFAULT_CONFIG["margin_calculation"]
        
        for key, default_value in default_margin_config.items():
            try:
                value = config.get(key, default_value)
                
                # Convert to float and validate range
                float_value = float(value)
                
                # Validate reasonable ranges for each parameter
                if key in ['commission_percent', 'acquiring_percent', 'advertising_percent', 'vat_percent']:
                    if not (0 <= float_value <= 100):
                        print(f"DEBUG: {key} value {float_value} out of range [0,100], using default {default_value}")
                        validated_config[key] = default_value
                    else:
                        validated_config[key] = float_value
                elif key == 'exchange_rate':
                    if not (1 <= float_value <= 1000):
                        print(f"DEBUG: {key} value {float_value} out of range [1,1000], using default {default_value}")
                        validated_config[key] = default_value
                    else:
                        validated_config[key] = float_value
                else:
                    validated_config[key] = float_value
                    
            except (ValueError, TypeError) as e:
                print(f"DEBUG: Invalid value for {key}: {config.get(key)}, error: {e}, using default {default_value}")
                validated_config[key] = default_value
        
        update_config_value("margin_calculation", validated_config)
        
    except Exception as e:
        print(f"DEBUG: Error setting margin config: {e}")
        if hasattr(st, 'error'):
            st.error(f"Ошибка при сохранении конфигурации маржинальности: {e}")

def get_margin_parameter(param_name: str, default: float = None) -> float:
    """
    Returns a specific margin calculation parameter value.
    Enhanced with validation and error handling.
    
    Args:
        param_name (str): Name of the parameter (commission_percent, acquiring_percent, 
                         advertising_percent, vat_percent, exchange_rate)
        default (float, optional): Default value if parameter not found. 
                                  If None, uses value from DEFAULT_CONFIG.
    
    Returns:
        float: The parameter value.
    """
    try:
        if default is None:
            default = DEFAULT_CONFIG["margin_calculation"].get(param_name, 0.0)
        
        # Validate parameter name
        valid_params = ['commission_percent', 'acquiring_percent', 'advertising_percent', 'vat_percent', 'exchange_rate']
        if param_name not in valid_params:
            print(f"DEBUG: Invalid margin parameter name: {param_name}")
            return float(default)
        
        value = get_config_value("margin_calculation", sub_key=param_name, default=default)
        
        # Validate and convert to float
        try:
            float_value = float(value)
            
            # Validate reasonable ranges
            if param_name in ['commission_percent', 'acquiring_percent', 'advertising_percent', 'vat_percent']:
                if not (0 <= float_value <= 100):
                    print(f"DEBUG: {param_name} value {float_value} out of range, using default {default}")
                    return float(default)
            elif param_name == 'exchange_rate':
                if not (1 <= float_value <= 1000):
                    print(f"DEBUG: {param_name} value {float_value} out of range, using default {default}")
                    return float(default)
            
            return float_value
            
        except (ValueError, TypeError):
            print(f"DEBUG: Invalid {param_name} value: {value}, using default {default}")
            return float(default)
        
    except Exception as e:
        print(f"DEBUG: Error getting margin parameter {param_name}: {e}")
        return float(default) if default is not None else 0.0

def set_margin_parameter(param_name: str, value: float) -> None:
    """
    Sets and saves a specific margin calculation parameter.
    Enhanced with validation and error handling.
    
    Args:
        param_name (str): Name of the parameter (commission_percent, acquiring_percent,
                         advertising_percent, vat_percent, exchange_rate)
        value (float): The new parameter value.
    """
    try:
        # Validate parameter name
        valid_params = ['commission_percent', 'acquiring_percent', 'advertising_percent', 'vat_percent', 'exchange_rate']
        if param_name not in valid_params:
            raise ValueError(f"Invalid parameter name: {param_name}. Valid parameters: {valid_params}")
        
        # Validate and convert value
        try:
            float_value = float(value)
        except (ValueError, TypeError):
            raise ValueError(f"Parameter value must be numeric, got {type(value)}: {value}")
        
        # Validate reasonable ranges
        if param_name in ['commission_percent', 'acquiring_percent', 'advertising_percent', 'vat_percent']:
            if not (0 <= float_value <= 100):
                raise ValueError(f"{param_name} must be between 0 and 100, got {float_value}")
        elif param_name == 'exchange_rate':
            if not (1 <= float_value <= 1000):
                raise ValueError(f"{param_name} must be between 1 and 1000, got {float_value}")
        
        update_config_value("margin_calculation", float_value, sub_key=param_name)
        
    except Exception as e:
        print(f"DEBUG: Error setting margin parameter {param_name}: {e}")
        if hasattr(st, 'error'):
            st.error(f"Ошибка при сохранении параметра {param_name}: {e}")
        raise

if __name__ == '__main__':
    # Example usage and basic test for configuration utilities
    print("--- Testing config_utils.py ---")
    print("\n1. Loading initial config (or creating default if not exists):")
    cfg = load_config()
    print(json.dumps(cfg, indent=2))

    print("\n2. Getting specific values:")
    print(f"   Current DB path: {get_db_path()}")
    print(f"   Current oz_orders_csv path: {get_report_path('oz_orders_csv')}")

    print("\n3. Updating DB path:")
    new_db_path = "new_path/data_test.db"
    print(f"   Setting DB path to: '{new_db_path}'")
    set_db_path(new_db_path)
    print(f"   New DB path from get_db_path(): {get_db_path()}")

    print("\n4. Updating a report path:")
    new_report_key = "oz_products_csv"
    new_report_path_val = "test_reports/ozon/products_test.csv"
    print(f"   Setting {new_report_key} path to: '{new_report_path_val}'")
    set_report_path(new_report_key, new_report_path_val)
    print(f"   New {new_report_key} path: {get_report_path(new_report_key)}")
    
    print("\n5. Re-loading config to verify persistence:")
    cfg = load_config()
    print(json.dumps(cfg, indent=2))

    # Reset to default for next run if needed
    # save_config(DEFAULT_CONFIG)
    # print("\nConfig reset to default by saving DEFAULT_CONFIG.")
    # print("--- config_utils.py Test Complete ---") 
