"""Launch the local UI with no database credentials in the child environment."""
import argparse
import os
from pathlib import Path
import subprocess
import sys

from orchestration.settings import ConfigError,load_config


def ui_environment(config_path,environment):
    clean={k:v for k,v in environment.items() if not k.startswith('RETAIL_DB_') and k not in ('MYSQL_PWD','MARIADB_PWD')}
    clean['RETAIL_UI_CONFIG']=str(Path(config_path).resolve())
    return clean


def main():
    parser=argparse.ArgumentParser(description='Retail Lab read-only Streamlit UI')
    parser.add_argument('--config',required=True,type=Path)
    args=parser.parse_args()
    try:config=load_config(args.config)
    except ConfigError:parser.exit(2,'Cannot read the explicit local UI configuration.\n')
    command=[sys.executable,'-m','streamlit','run',str(Path(__file__).with_name('main.py')),
        '--server.address='+config['ui']['host'],'--server.port='+str(config['ui']['port']),
        '--server.headless=true','--server.fileWatcherType=none','--browser.gatherUsageStats=false',
        '--theme.base=light','--theme.primaryColor=#3159D6','--theme.backgroundColor=#F2F5FA',
        '--theme.secondaryBackgroundColor=#F7F9FC','--theme.textColor=#1D2B45','--theme.font=sans serif']
    try:
        raise SystemExit(subprocess.call(command,env=ui_environment(args.config,os.environ)))
    except KeyboardInterrupt:
        pass


if __name__=='__main__':main()
