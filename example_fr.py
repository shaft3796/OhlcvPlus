# Import d'OhlcvPlus.
from ohlcv.ohlcv import OhlcvPlus
# Import de ccxt.
import ccxt

"""
Nous créons d'abord notre client ccxt, plus d'informations sur https://github.com/ccxt/ccxt.
Bien que ccxt permette d'utiliser beaucoup d'échanges, il est important de garder à l'esprit que certains peuvent avoir
un comportement inattendu..
"""
client = ccxt.binance()

"""
Nous créons ensuite notre instance OhlcvPlus.
Le premier paramètre est le client ccxt instancié ci-dessus.
Le deuxième paramètre est optionnel, OhlcvPlus utilise une base de données sqlite pour stocker les données, cette base
de données est stockée dans un fichier, ce paramètre est le chemin vers ce fichier, si le fichier n'existe pas il sera
créé. Vous pouvez passer None à ce paramètre pour désactiver la persistance des données.
"""
ohlcvp = OhlcvPlus(client, database_path='my_data.db')

"""
Nous sommes maintenant en mesure de télécharger notre premier dataframe. La classe OhlcvPlus dispose d'une méthode de
téléchargement, mais nous vous recommandons d'utiliser uniquement la méthode load, qui est un wrapper autour de la
méthode de téléchargement, mais avec quelques fonctionnalités supplémentaires.
Lorsqu'elle est appelée pour la première fois, cette méthode téléchargera le ohlcv, créera un champ dans la base de
données, l'enregistrera (si la base de données est activée) et renverra le dataframe. Lorsqu'elle est appelée à nouveau,
le ohlcv sera directement chargé depuis la base de données et mis à jour avec de nouvelles bougies si nécessaire.
Le premier paramètre est le marché, par exemple 'BTC/USDT'. Ce paramètre est directement transmis à CCXT, si vous
rencontrez une erreur, veuillez vous référer à la documentation CCXT.
Le deuxième paramètre est le timeframe, par exemple '1m'. Ce paramètre est directement transmis à CCXT, si vous
rencontrez une erreur, veuillez vous référer à la documentation CCXT.
Le troisième paramètre est la date à partir de laquelle vous souhaitez télécharger le ohlcv, par exemple
'2021-01-01 00:00:00'. Si aucune donnée n'est disponible pour cette date, une erreur sera levée.
Le quatrième paramètre est la limite, ce paramètre est utilisé pour limiter le nombre de bougies téléchargées. La
première valeur possible est un entier indiquant le nombre de bougies à télécharger, par exemple 1000. La deuxième
valeur possible est une date, par exemple '2021-01-01 00:00:00', dans ce cas, le téléchargement s'arrêtera lorsque la
date sera atteinte. La troisième valeur possible est -1, dans ce cas, le téléchargement s'arrêtera lorsqu'il n'y aura
plus de données disponibles.
Le cinquième paramètre est le paramètre update, s'il est défini sur True, le ohlcv sera mis à jour avec les dernières
bougies lorsqu'il est chargé depuis la base de données, c'est utile pour garder facilement des données à jour. Si vous
définissez ce paramètre sur True mais que le paramètre limit est défini sur une date ou un entier autre que -1, les
données seront toujours mises à jour et enregistrées mais le dataframe sera renvoyé en fonction du paramètre limit.
Le sixième paramètre est le paramètre verbose, s'il est défini sur True, la progression du téléchargement sera affichée.
Le septième paramètre est le paramètre workers, utilisez ce paramètre avec prudence, référez-vous à la docstring de la
méthode de téléchargement.
Pour plus d'informations sur la méthode de téléchargement, référez-vous à la docstring de la méthode de téléchargement.
"""
# Download 1000 candles from 2023-01-01 00:00:00 to 2023-02-01 00:00:00.
ohlcv1 = ohlcvp.load(market='BTC/USDT', timeframe='1m', since='2023-01-01 00:00:00', limit=1000, update=True, verbose=True, workers=100)
# Download all candles from 2023-01-01 00:00:00 to 2023-02-01 00:00:00.
ohlcv2 = ohlcvp.load(market='BTC/USDT', timeframe='1m', since='2023-01-01 00:00:00', limit='2023-02-01 00:00:00', update=True, verbose=True, workers=100)
# Download all candles from 2023-01-01 00:00:00 to now.
ohlcv3 = ohlcvp.load(market='BTC/USDT', timeframe='1m', since='2023-01-01 00:00:00', limit=-1, update=True, verbose=True, workers=100)

# Same as the first request, but this one the ohlcv will be loaded and updated from the database.
ohlcv4 = ohlcvp.load(market='BTC/USDT', timeframe='1m', since='2023-01-01 00:00:00', limit=1000, update=True, verbose=True, workers=100)