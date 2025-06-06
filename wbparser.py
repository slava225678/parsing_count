import asyncio
import logging
from json import JSONDecodeError, loads

import pandas as pd
from aiohttp import (
    ClientConnectorError,
    ClientSession,
    ServerDisconnectedError
)
from tqdm.asyncio import tqdm_asyncio

from constants import (
    INPUT_FILE,
    OUTPUT_FILE,
    NULL,
    REQ_COUNT_FROM,
    REQ_COUNT_TO
)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def fetch_product_count(
        session: ClientSession,
        query: str
) -> int | None:
    """
    Отправляет асинхронный GET-запрос к Wildberries
    и возвращает количество товаров по запросу.
    """
    url = (
        "https://search.wb.ru/exactmatch/ru/common/v7/search?"
        "ab_testing=false&appType=1&curr=rub&dest=-1257786&query="
        f"{query}&resultset=catalog&sort=popular&spp=30&"
        "suppressSpellcheck=true&uclusters=0"
    )
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            text_data = await response.text()
            try:
                data = loads(text_data)
                return data.get("data", {}).get("total", 0)
            except JSONDecodeError as e:
                logger.error("Ошибка при разборе JSON для '%s': %s", query, e)
                return NULL
    except (ServerDisconnectedError, ClientConnectorError, Exception) as e:
        logger.error("Ошибка при запросе для '%s': %s", query, e)
        return NULL


async def process_requests(
    data: pd.DataFrame,
    batch_size: int,
    sleep_duration: int
) -> list[int | None]:
    """
    Асинхронно обрабатывает батчи запросов и делает паузы между ними.

    :param data: DataFrame с колонками 'Запрос' и 'Кол-во запросов'
    :param batch_size: Максимальный размер батча запросов
    :param sleep_duration: Пауза между батчами в секундах
    :return: Список количества товаров по каждому запросу
    """
    results = []
    tasks = []

    for _, row in data.iterrows():
        query = row['Запрос']
        request_count = row['Кол-во запросов']

        if not isinstance(query, str) or not query.strip():
            continue

        if not (REQ_COUNT_FROM <= request_count <= REQ_COUNT_TO):
            continue

        if query.replace(" ", "").isdecimal():
            continue

        tasks.append(query)

        if len(tasks) >= batch_size:
            async with ClientSession() as session:
                product_counts = await tqdm_asyncio.gather(
                    *[fetch_product_count(session, q) for q in tasks]
                )
                results.extend(product_counts)
                logger.info("Пауза %d секунд...", sleep_duration)
                await asyncio.sleep(sleep_duration)
                tasks = []

    if tasks:
        async with ClientSession() as session:
            product_counts = await tqdm_asyncio.gather(
                *[fetch_product_count(session, q) for q in tasks]
            )
            results.extend(product_counts)

    return results


async def main(batch_size: int = 5000, sleep_duration: int = 30):
    """
    Загружает запросы из CSV, запускает
    асинхронный парсинг, сохраняет результат в Excel.
    """
    data = pd.read_csv(
        INPUT_FILE, header=None, names=['Запрос', 'Кол-во запросов']
    )
    product_counts = await process_requests(data, batch_size, sleep_duration)

    results = []
    for _, row in data.iterrows():
        query = row['Запрос']
        request_count = row['Кол-во запросов']

        if (REQ_COUNT_FROM <= request_count <= REQ_COUNT_TO
                and isinstance(query, str)
                and query.strip()
                and not query.replace(" ", "").isdecimal()):
            results.append({
                'Запрос': query,
                'Кол-во запросов': request_count,
                'Кол-во товаров': product_counts.pop(0)
            })

    pd.DataFrame(results).to_excel(OUTPUT_FILE, index=False)
    logger.info("Результаты сохранены в файл: %s", OUTPUT_FILE)


if __name__ == "__main__":
    asyncio.run(main(batch_size=100, sleep_duration=1))
