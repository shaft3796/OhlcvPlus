from setuptools import setup

setup(name='OhlcvPlus',
      version='1.0',
      description='Crypto OHLCV data downloader.',
      author='Shaft',
      url='https://github.com/Shaft-3796/OHLCV-Plus',
      download_url='https://github.com/Shaft-3796/OHLCV-Plus/archive/refs/tags/2.0.0.tar.gz',
      packages=['ohlcv'],
      install_requires=['ccxt', 'pandas', 'colorama', 'SQLAlchemy'])