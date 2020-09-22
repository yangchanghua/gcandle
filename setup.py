
from setuptools import setup, find_packages

NAME = "gcandle"
DESCRIPTION = "gcandle: golden candle sticks"
KEYWORDS = ["gcandle", "quant", "finance", "Backtest", 'Framework']
VERSION='0.0.1'
LICENSE = "MIT"
required = ['pandas>=0.23.4', 'numpy>=1.12.0', 'tushare', 'flask_socketio>=2.9.0 ', 'motor>=1.1', 'seaborn>=0.8.1', 'pyconvert>=0.6.3',
                      'lxml>=4.0', ' beautifulsoup4', 'matplotlib', 'requests', 'tornado', 'janus', 'pyecharts_snapshot', 'async_timeout',
                      'demjson>=2.2.4', 'pymongo>=3.7', 'six>=1.10.0', 'tabulate>=0.7.7', 'pytdx>=1.67', 'retrying>=1.3.3',
                      'zenlog>=1.1', 'delegator.py>=0.0.12', 'flask>=0.12.2', 'pyecharts==0.5.11', 'protobuf>=3.4.0']

setup(
    name=NAME,
    description=DESCRIPTION,
    version=VERSION,
    # install_requires=required,
    license=LICENSE,
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    include_package_data=True,
)
