import json


class Settings:
    _settings = None
    _settings_file_name = "settings.json"

    @classmethod
    def load_settings(cls):
        if cls._settings is None:
            with open(cls._settings_file_name, "r") as settings_file:
                cls._settings = json.load(settings_file)

    @classmethod
    def get_setting(cls, setting_key):
        if cls._settings is None:
            cls.load_settings()
        return cls._settings.get(setting_key)

