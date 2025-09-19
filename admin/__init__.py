import os

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
PROJECT_PATH = os.path.join(DIR_PATH, os.pardir)
BLOCKSCOUT_PATH = os.path.join(PROJECT_PATH, 'deps', 'blockscout', 'docker-compose')
SERVER_DATA_DIR = os.path.join(PROJECT_PATH, 'data')
ABI_FILEPATH = os.path.join(SERVER_DATA_DIR, 'abi.json')
GAS_PRICES_FILEPATH = os.path.join(SERVER_DATA_DIR, 'gas_prices.csv')
EXPLORERS_META_DATA_PATH = os.path.join(SERVER_DATA_DIR, 'meta.json')
SCHAIN_CONFIG_DIR_PATH = os.path.join(SERVER_DATA_DIR, 'configs')

DOCKER_COMPOSE_BIN_PATH = '/usr/bin/docker'
DUMPS_DIR_PATH = os.path.join(SERVER_DATA_DIR, 'dumps')
ENVS_DIR_PATH = os.path.join(SERVER_DATA_DIR, 'envs')


HOST_DIR_PATH = os.environ.get('HOST_DIR_PATH')
HOST_SCHAIN_CONFIG_DIR_PATH = os.path.join(HOST_DIR_PATH, 'data', 'configs')
HOST_SSL_DIR_PATH = os.path.join(HOST_DIR_PATH, 'data', 'certs')
BLOCKSCOUT_DATA_DIR = os.path.join(HOST_DIR_PATH, 'data', 'blockscout-data')
BLOCKSCOUT_PROXY_CONFIG_DIR = os.path.join(HOST_DIR_PATH, 'deps', 'blockscout',
                                           'docker-compose', 'proxy')
BLOCKSCOUT_PROXY_SSL_CONFIG_DIR = os.path.join(HOST_DIR_PATH, 'deps', 'blockscout',
                                               'docker-compose', 'proxy-ssl')


NGINX_CONFIGS_DIR = os.path.join(SERVER_DATA_DIR, 'nginx')
EXPLORERS_NGINX_CONFIG_PATH = os.path.join(NGINX_CONFIGS_DIR, 'nginx.conf')
STATS_NGINX_CONFIG_PATH = os.path.join(NGINX_CONFIGS_DIR, 'stats.conf')
DOCKER_COMPOSE_CONFIG_NAME = 'docker-compose.yml'

ENDPOINT = os.environ.get('ETH_ENDPOINT')
ETH_API_KEY = os.environ.get('ETH_API_KEY')
PROXY_DOMAIN_NAME = os.environ.get('PROXY_DOMAIN')
SCHAIN_NAMES = os.environ.get('SCHAIN_NAMES')
FROM_FIRST_BLOCK = True if os.environ.get('FROM_FIRST_BLOCK') else False
SSL_ENABLED = True if os.environ.get('SSL_ENABLED') == 'true' else False
HOST_DOMAIN = os.environ.get('INTERNAL_DOMAIN_NAME')
IS_TESTNET = True if os.environ.get('IS_TESTNET') == 'true' else False
WALLET_CONNECT_PROJECT_ID = os.environ.get('WALLET_CONNECT_PROJECT_ID')
BLOCKSCOUT_BACKEND_DOCKER_TAG = os.environ.get('BLOCKSCOUT_BACKEND_DOCKER_TAG')
BLOCKSCOUT_FRONTEND_DOCKER_TAG = os.environ.get('BLOCKSCOUT_FRONTEND_DOCKER_TAG')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
RE_CAPTCHA_SECRET_KEY = os.environ.get('RE_CAPTCHA_SECRET_KEY')
NOVES_API_KEY = os.environ.get('NOVES_API_KEY')
USE_HTTP_ENDPOINT = True if os.environ.get('USE_HTTP_ENDPOINT') == 'true' else False
PUBLIC_IP = os.environ.get('PUBLIC_IP')
NETWORK_NAME = os.environ.get('NETWORK_NAME', 'skale')
STATIC_BLOCK_REWARD = os.environ.get('STATIC_BLOCK_REWARD', '2500000000000000000')
BURNT_FEE_FRACTION = os.environ.get('BURNT_FEE_FRACTION', '0.5')

if NETWORK_NAME == 'fair':
    DOCKER_COMPOSE_CONFIG_NAME = 'docker-compose.fair.yml'

DOCKER_COMPOSE_CONFIG_PATH = os.path.join(BLOCKSCOUT_PATH,
                                          DOCKER_COMPOSE_CONFIG_NAME)

SSL_DIR_PATH = os.path.join(SERVER_DATA_DIR, 'certs')
SSL_CRT_PATH = os.path.join(SSL_DIR_PATH, 'server.crt')
SSL_KEY_PATH = os.path.join(SSL_DIR_PATH, 'server.key')
DB_FILE_PATH = os.path.join(SERVER_DATA_DIR, 'new_stats.db')

PROXY_ADMIN_PREDEPLOYED_ADDRESS = '0xD1000000000000000000000000000000000000D1'
SCHAIN_OWNER_ALLOC = 1000000000000000000000000000000
ETHERBASE_ALLOC = 57896044618658097711785492504343953926634992332820282019728792003956564819967
NODE_OWNER_ALLOC = 1000000000000000000000000000000
BASE_ADDRESS = '0x0000000000000000000000000000000000000001'
ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'

STATS_TIME_DELTA = 3600
GAS_PRICE_REFRESHING_TIME = 86400
COMPOSE_HTTP_TIMEOUT = 600

NOVES_SUPPORTED_CHAINS = {
    "honorable-steel-rasalhague": "skale-calypso-hub",
    "elated-tan-skat": "skale-europa-hub",
    "green-giddy-denebola": "skale-nebula-hub",
    "parallel-stormy-spica": "skale-titan-hub",
    "giant-half-dual-testnet": "skale-calypso-hub-testnet",
    "juicy-low-small-testnet": "skale-europa-hub-testnet",
    "lanky-ill-funny-testnet": "skale-nebula-hub-testnet",
    "aware-fake-trim-testnet": "skale-titan-hub-testnet"
}
