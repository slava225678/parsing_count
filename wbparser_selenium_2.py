import asyncio
import json
import logging

from concurrent.futures import ThreadPoolExecutor
import os
import time
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import (
    WebDriverException
)
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from constants import (
    INPUT_FILE,
    OUTPUT_FILE,
    NULL,
    REQ_COUNT_FROM,
    REQ_COUNT_TO,
    BROWSER_COUNT,
    BATCH_SIZE,
    SLEEP_DURATION,
    AUT0_SAVE_EVERY,
    AVG_REQ_PER_DAY_PREV_PERIOD,
    AVG_REQ_PER_DAY,
    COUNT_REQ_PREV_PERIOD,
    COUNT_REQ,
    AVG_PRICE_RUB,
    COUNT_PRODUCTS,
    SEARCH_QUER,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def setup_driver():
    """Настройка и создание драйвера Chrome"""
    chrome_options = Options()
    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled"
    )
    chrome_options.add_experimental_option(
        "excludeSwitches", ["enable-automation"]
    )
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    # Для ускорения можно использовать headless, но WB может это детектить
    chrome_options.add_argument("--headless=new")
    # Отключаем logging
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--silent")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    return driver


def fetch_batch_selenium(
        queries: list[str]
) -> dict[str, tuple[int | None, float | None]]:
    """Обработка нескольких запросов в одном окне браузера."""
    driver = None
    results = {}

    try:
        driver = setup_driver()
        driver.get("about:blank")
        time.sleep(2)

        for query in queries:
            url = (
                "https://www.wildberries.ru/__internal/"
                "search/exactmatch/ru/common/v18/search"
                "?ab_testing=false&appType=1&curr=rub"
                "&dest=-1116490&hide_dtype=9;11"
                "&hide_vflags=4294967296&inheritFilters=false&lang=ru&page=1"
                f"&query={query}&resultset=catalog&sort=popular&spp=30"
                "&suppressSpellcheck=false&uclusters=3"
            )

            driver.get(url)
            time.sleep(1.5)

            raw = driver.execute_script("return document.body.innerText;")

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                results[query] = (None, None)
                continue

            total = data.get("total")
            products = data.get("products", [])

            prices = []
            for product in products[:10]:
                sizes = product.get("sizes", [])
                if not sizes:
                    continue

                price_info = None
                for s in sizes:
                    if isinstance(s, dict) and "price" in s:
                        price_info = s["price"]
                        break

                if not price_info:
                    continue

                price_value = price_info.get("product")
                if price_value:
                    prices.append(price_value / 100)

            avg_price = sum(prices) / len(prices) if prices else None
            results[query] = (total, avg_price)

            driver.execute_script("window.scrollTo(0, 400)")
            time.sleep(0.2)
        return results

    finally:
        if driver:
            try:
                driver.quit()
            except WebDriverException:
                pass


async def fetch_batch(
        queries: list[str]
) -> dict[str, tuple[int | None, float | None]]:
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=BROWSER_COUNT) as executor:
        result = await loop.run_in_executor(
            executor,
            fetch_batch_selenium,
            queries
        )
    return result


async def process_requests(
        data: pd.DataFrame,
        batch_size: int,
        sleep_duration: int,
        resume: bool = True,
        partial_every: int = 1000
) -> dict[str, tuple[int | None, float | None]]:

    partial_file = OUTPUT_FILE + ".partial.xlsx"

    # загружаем частичный файл, если есть
    processed_queries = set()
    all_results: dict[str, tuple[int | None, float | None]] = {}

    if resume and os.path.exists(partial_file):
        df = pd.read_excel(partial_file)
        for _, r in df.iterrows():
            q = str(r["query"])
            processed_queries.add(q)
            all_results[q] = (r["total"], r["avg_price"])

        logger.info(
            f"Возобновление: найдено {len(processed_queries)}"
            "ранее обработанных запросов"
        )

    batch: list[str] = []
    processed_count = len(processed_queries)

    for _, row in data.iterrows():
        query = row[SEARCH_QUER]
        request_count = row[COUNT_REQ]

        if not isinstance(query, str) or not query.strip():
            continue
        if not (REQ_COUNT_FROM <= request_count <= REQ_COUNT_TO):
            continue
        if query.replace(" ", "").isdecimal():
            continue
        if query in processed_queries:
            continue

        batch.append(query)

        if len(batch) >= batch_size:
            batch_result = await fetch_batch(batch)
            all_results.update(batch_result)

            processed_count += len(batch)

            # частичное сохранение
            if processed_count % partial_every == 0:
                df_partial = pd.DataFrame(
                    [
                        {"query": q, "total": r[0], "avg_price": r[1]}
                        for q, r in all_results.items()
                    ]
                )
                df_partial.to_excel(partial_file, index=False)
                logger.info(
                    f"Промежуточное сохранение ({processed_count} записей)"
                )

            logger.info(
                f"Батч обработан ({len(batch)}). Пауза {sleep_duration} сек..."
            )
            await asyncio.sleep(sleep_duration)
            batch = []

    # финальный батч
    if batch:
        batch_result = await fetch_batch(batch)
        all_results.update(batch_result)
        processed_count += len(batch)

        df_partial = pd.DataFrame(
            [
                {"query": q, "total": r[0], "avg_price": r[1]}
                for q, r in all_results.items()
            ]
        )
        df_partial.to_excel(partial_file, index=False)
        logger.info(
            f"Финальное промежуточное сохранение ({processed_count} записей)"
        )

    return all_results


async def main(
        batch_size: int = 5,
        sleep_duration: int = 3,
        resume: bool = True,
        partial_every: int = 1000
):
    """Основная функция"""
    # Читаем данные
    data = pd.read_excel(INPUT_FILE)
    logger.info(f"Загружено {len(data)} записей из файла")
    # Получаем данные
    product_data = await process_requests(
        data=data,
        batch_size=batch_size,
        sleep_duration=sleep_duration,
        resume=resume,
        partial_every=partial_every
    )
    # Формируем результаты
    results = []
    for _, row in data.iterrows():
        query = row[SEARCH_QUER]
        request_count = row[COUNT_REQ]
        if (REQ_COUNT_FROM <= request_count <= REQ_COUNT_TO
                and isinstance(query, str)
                and query.strip()
                and not query.replace(" ", "").isdecimal()):
            total, avg_price = product_data.get(query, (None, None))
            results.append({
                SEARCH_QUER: query,
                COUNT_REQ: request_count,
                COUNT_PRODUCTS: total,
                AVG_PRICE_RUB: avg_price,
                COUNT_REQ_PREV_PERIOD: row.get(
                    COUNT_REQ_PREV_PERIOD, NULL
                ),
                AVG_REQ_PER_DAY: row.get(
                    AVG_REQ_PER_DAY, NULL
                ),
                AVG_REQ_PER_DAY_PREV_PERIOD: row.get(
                    AVG_REQ_PER_DAY_PREV_PERIOD, NULL
                )
            })
    # Создаем DataFrame и сохраняем
    df = pd.DataFrame(results)[[
        SEARCH_QUER,
        COUNT_REQ,
        COUNT_PRODUCTS,
        AVG_PRICE_RUB,
        COUNT_REQ_PREV_PERIOD,
        AVG_REQ_PER_DAY,
        AVG_REQ_PER_DAY_PREV_PERIOD
    ]]

    df.to_excel(OUTPUT_FILE, index=False)
    logger.info(f"Результаты сохранены в файл: {OUTPUT_FILE}")
    logger.info(f"Обработано {len(results)} записей")


if __name__ == "__main__":
    asyncio.run(
        main(
            batch_size=BATCH_SIZE,
            sleep_duration=SLEEP_DURATION,
            partial_every=AUT0_SAVE_EVERY
        )
    )
