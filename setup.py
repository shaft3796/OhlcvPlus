from setuptools import setup
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(name='ohlcv-plus',
      version='2.0.2',
      description='Crypto OHLCV data downloader.',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Shaft',
      url='https://github.com/Shaft-3796/OHLCV-Plus',
      download_url='https://github.com/Shaft-3796/OHLCV-Plus/archive/refs/tags/2.0.0.tar.gz',
      packages=['ohlcv'],
      install_requires=['ccxt', 'pandas', 'colorama', 'SQLAlchemy'])