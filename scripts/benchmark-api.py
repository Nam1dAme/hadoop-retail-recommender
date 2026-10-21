#!/usr/bin/env python3
"""Execute the frozen NF-02 protocol against the actual local API."""
import argparse
import json
from pathlib import Path
from orchestration.api_performance import measure
from orchestration.database import connect
from orchestration.settings import load_config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--db-defaults-file', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    config = load_config(args.config)
    connection = connect(config, args.db_defaults_file)
    try:
        result = measure(connection, config, args.output_dir)
        print(json.dumps(dict(status=result['status'], classes=len(result['results']), output=str(args.output_dir))))
        return 0 if result['status'] == 'PASS' else 1
    finally:
        connection.close()


if __name__ == '__main__':
    raise SystemExit(main())
