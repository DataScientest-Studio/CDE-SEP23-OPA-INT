import configparser


class ConfigManager:
    """Class to manage the config file"""

    def __init__(self, config_path):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

    def get_value(self, section, key):
        """Get a value from the config file"""
        return self.config[section][key]
