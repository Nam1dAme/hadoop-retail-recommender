"""Start the loopback-only W09 API with explicitly supplied reader credentials."""
import argparse
from pathlib import Path
import time

import uvicorn
from pymysql import MySQLError

from orchestration.database import connect
from orchestration.migrations import MigrationError
from orchestration.settings import load_config, ConfigError
from service.api import create_app
from service.database import ReadSession
from service.errors import APIError


def main():
    parser = argparse.ArgumentParser(description='Retail read-only API (HL-DOC-1.0.0)')
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--db-defaults-file', type=Path)
    args = parser.parse_args()
    try:
        config = load_config(args.config)
        with ReadSession(lambda: connect(config, args.db_defaults_file,
                         connect_timeout=2, read_timeout=2, write_timeout=2), time.monotonic()+5):
            pass
        app = create_app(config, args.db_defaults_file)
    except (ConfigError, MySQLError, MigrationError, APIError):
        parser.exit(2, 'Service startup checks failed; inspect explicit configuration, reader grants and schema.\n')
    # No host override or forwarded-header trust; no raw URL access logging.
    uvicorn.run(app, host=config['api']['host'], port=config['api']['port'],
                proxy_headers=False, access_log=False, log_level='info')


if __name__ == '__main__':
    main()
