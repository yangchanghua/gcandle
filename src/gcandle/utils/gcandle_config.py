import json
import os


MONGO_URI = 'mongodb://localhost:27017'
DATABASE_NAME = 'gcandle'


class GcandleConfig:
    def __init__(self):
        self.mongodb_uri = MONGO_URI
        self.mongodb_name = DATABASE_NAME
        self.load_config()

    def set_mongodb_uri(self, uri):
        self.mongodb_uri = uri

    def set_mongodb_name(self, name):
        self.mongodb_name = name

    def load_config(self):
        CONFIG_FILE = os.getenv('GCANDLE_CONFIG_FILE')
        if CONFIG_FILE is None:
            HOME_DIR = os.getenv("HOME")
            if HOME_DIR is None:
                print('Cannot get $HOME dir, default config will be used.')
                return self
            CONFIG_FILE = HOME_DIR + '/' + '.gcandle.json'
        try:
            with open(CONFIG_FILE, 'r') as config_file:
                data = json.load(config_file)
                print("Config data: {}".format(data))
                if "mongodb_uri" in data:
                    self.mongodb_uri = data['mongodb_uri']
                if "mongodb_name" in data:
                    self.mongodb_name = data['mongodb_name']
        except Exception as e:
            print('Failed to open config file {} with error: {}'
                  .format(CONFIG_FILE, e))
            print(' default config will be used')


GCANDLE_CONFIG = GcandleConfig()


if __name__ == '__main__':
    print(GCANDLE_CONFIG.mongodb_uri, GCANDLE_CONFIG.mongodb_name)
