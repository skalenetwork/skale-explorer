import logging

from flask import Flask, request

from admin import FLASK_APP_PORT, FLASK_APP_HOST
from admin.configs.nginx import generate_stats_nginx_config
from admin.core.containers import restart_nginx
from admin.statistics.database import StatsRecord
from admin.utils.logger import init_logger
from admin.utils.web import construct_ok_response

logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route("/stats")
def get_stats():
    logger.debug(request)
    data = StatsRecord.get_last_stats()
    return construct_ok_response(data)


def main():
    logger.info('Generating nginx config for stats server')
    generate_stats_nginx_config()
    restart_nginx()
    logger.info('Starting Flask server')
    app.run(port=FLASK_APP_PORT, host=FLASK_APP_HOST)


if __name__ == '__main__':
    init_logger()
    main()
