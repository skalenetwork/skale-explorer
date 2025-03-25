import fnmatch
import os
from admin import (EXPLORERS_NGINX_CONFIG_PATH, SSL_CRT_PATH, SSL_KEY_PATH,
                   ENVS_DIR_PATH, SSL_ENABLED)
import crossplane
from admin.utils.helper import read_env_file


def generate_schain_nginx_config(schain_name, explorer_endpoint, ssl=False):
    config = generate_base_nginx_config(schain_name, explorer_endpoint)
    if ssl:
        ssl_block = [
            {
                "directive": "listen",
                "args": [
                    '443',
                    'ssl'
                ]
            },
            {
                "directive": "ssl_certificate",
                "args": [
                    '/data/server.crt'
                ]
            },
            {
                "directive": "ssl_certificate_key",
                "args": [
                    '/data/server.key'
                ]
            }
        ]
        config['block'] = ssl_block + config['block']
    return config


def insert_proxy_headers(config):
    headers = [
        {"directive": "proxy_buffer_size", "args": ["128k"]},
        {"directive": "proxy_buffers", "args": ["4", "256k"]},
        {"directive": "proxy_busy_buffers_size", "args": ["256k"]},
        {"directive": "proxy_temp_file_write_size", "args": ["256k"]}
    ]
    index = None
    for i, directive in enumerate(config['block']):
        if directive['directive'] == "server_name":
            index = i
            break
    if index is None:
        index = len(config['block'])
    for header in reversed(headers):
        config['block'].insert(index + 1, header)
    return config


def generate_base_nginx_config(schain_name, explorer_endpoint):
    config = {
        "directive": "server",
        "args": [],
        "block": [
            {
                "directive": "listen",
                "args": [
                    '80'
                ]
            },
            {
                "directive": "server_name",
                "args": [
                    f"{schain_name}.*"
                ]
            },
            {
                "directive": "location",
                "args": [
                    "/socket"
                ],
                "block": [
                    {
                        "directive": "proxy_http_version",
                        "args": [
                            '1.1'
                        ]
                    },
                    {
                        "directive": "proxy_set_header",
                        "args": [
                            'Upgrade', '$http_upgrade'
                        ]
                    },
                    {
                        "directive": "proxy_set_header",
                        "args": [
                            'Connection', "upgrade"
                        ]
                    },
                    {
                        "directive": "proxy_pass",
                        "args": [
                            explorer_endpoint
                        ]
                    }
                ]
            },
            {
                "directive": "location",
                "args": [
                    "/"
                ],
                "block": [
                    {
                        "directive": "proxy_pass",
                        "args": [
                            explorer_endpoint
                        ]
                    }
                ]
            }
        ]
    }
    return insert_proxy_headers(config)


def generate_default_nginx_config(ssl=False):
    """
    Generates a default catch-all server block that returns a 404 error for any unmatched requests.
    """
    config = {
        "directive": "server",
        "args": [],
        "block": [
            {
                "directive": "listen",
                "args": [
                    '80',
                    'default_server'
                ]
            },
            {
                "directive": "server_name",
                "args": [
                    "_"
                ]
            },
            {
                "directive": "location",
                "args": [
                    "/"
                ],
                "block": [
                    {
                        "directive": "return",
                        "args": [
                            "404"
                        ]
                    }
                ]
            }
        ]
    }
    if ssl:
        ssl_block = [
            {
                "directive": "listen",
                "args": [
                    '443',
                    'ssl',
                    'default_server'
                ]
            },
            {
                "directive": "ssl_certificate",
                "args": [
                    '/data/server.crt'
                ]
            },
            {
                "directive": "ssl_certificate_key",
                "args": [
                    '/data/server.key'
                ]
            }
        ]
        config['block'] = ssl_block + config['block']
    return config


def regenerate_nginx_config():
    nginx_cfg = []
    for file in os.listdir(ENVS_DIR_PATH):
        if not fnmatch.fnmatch(file, '*.env'):
            continue
        schain_env = read_env_file(os.path.join(ENVS_DIR_PATH, file))
        schain_name = schain_env['SCHAIN_NAME']
        if SSL_ENABLED:
            proxy_endpoint = f'https://{schain_env["HOST"]}:{schain_env["PROXY_PORT"]}'
        else:
            proxy_endpoint = f'http://{schain_env["HOST"]}:{schain_env["PROXY_PORT"]}'
        if os.path.isfile(SSL_CRT_PATH) and os.path.isfile(SSL_KEY_PATH):
            schain_config = generate_schain_nginx_config(schain_name, proxy_endpoint, ssl=True)
        else:
            schain_config = generate_schain_nginx_config(schain_name, proxy_endpoint)
        nginx_cfg.append(schain_config)

    # Append the default catch-all server block to return a 404 for unmatched requests
    if SSL_ENABLED and os.path.isfile(SSL_CRT_PATH) and os.path.isfile(SSL_KEY_PATH):
        default_config = generate_default_nginx_config(ssl=True)
    else:
        default_config = generate_default_nginx_config(ssl=False)
    nginx_cfg.append(default_config)

    formatted_config = crossplane.build(nginx_cfg)
    with open(EXPLORERS_NGINX_CONFIG_PATH, 'w') as f:
        f.write(formatted_config)
