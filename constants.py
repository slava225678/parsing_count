NULL = 0
OUTPUT_FILE = 'parsing_coun/output/output.xlsx'
INPUT_FILE = 'parsing_coun/input/requests.xlsx'
REQ_COUNT_FROM = 10
REQ_COUNT_TO = 1000000

BROWSER_COUNT = 2  # Кол-во браузеров работающ-х одновремено (НЕ БОЛЬШЕ 2-х)
BATCH_SIZE = 30  # Кол-во запр. выполняющихся в одной сессии браузера
SLEEP_DURATION = 2  # Время сна между запросами Селениума
AUT0_SAVE_EVERY = 500  # Резервное сохранение каждые `n` запросов

AVG_REQ_PER_DAY_PREV_PERIOD = 'Запросов в среднем за день (предыдущий период)'
AVG_REQ_PER_DAY = 'Запросов в среднем за день'
COUNT_REQ_PREV_PERIOD = 'Количество запросов (предыдущий период)'
COUNT_REQ = 'Количество запросов'
AVG_PRICE_RUB = 'Средняя цена (₽)'
COUNT_PRODUCTS = 'Кол-во товаров'
SEARCH_QUER = 'Поисковый запрос'
