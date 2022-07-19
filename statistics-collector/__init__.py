import os

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
PROJECT_PATH = os.path.join(DIR_PATH, os.pardir)
SERVER_DATA_DIR = os.path.join(PROJECT_PATH, 'data')
EXPLORERS_META_DATA_PATH = os.path.join(SERVER_DATA_DIR, 'meta.json')