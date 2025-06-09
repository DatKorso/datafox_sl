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
    "report_paths": {
        "oz_barcodes_xlsx": "",
        "oz_orders_csv": "",
        "oz_prices_xlsx": "",
        "oz_products_csv": "",
        "wb_prices_xlsx": "",
        "wb_products_dir": "",
        "analytic_report_xlsx": ""
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