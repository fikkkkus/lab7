from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import urlparse
import aiohttp
import asyncio
import logging
import os
import pika

# Загрузка переменных окружения из файла
load_dotenv('params.env')

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

# Константы RabbitMQ
MQ_HOST = os.getenv("RABBITMQ_HOST")
MQ_PORT = int(os.getenv("RABBITMQ_PORT"))
MQ_USER = os.getenv("RABBITMQ_USER")
MQ_PASS = os.getenv("RABBITMQ_PASSWORD")
MQ_QUEUE = os.getenv("QUEUE_NAME")


def resolve_url(base, relative):
    """Создает абсолютный URL из базового и относительного."""
    if relative.startswith('http'):
        return relative

    parsed_base = urlparse(base)

    if relative.startswith('//'):
        return f'{parsed_base.scheme}:{relative}'

    if relative.startswith('/'):
        return f'{parsed_base.scheme}://{parsed_base.netloc}{relative}'

    if not base.endswith('/'):
        base += '/'

    return base + relative


async def fetch_content(target_url):
    """Получает ссылки и ресурсы с указанного адреса."""
    target_domain = urlparse(target_url).netloc

    async with aiohttp.ClientSession() as client:
        async with client.get(target_url) as response:
            if response.status != 200:
                logging.warning(f"Ошибка при запросе: {target_url}")
                return []

            page_content = await response.text()
            parsed_page = BeautifulSoup(page_content, "html.parser")
            page_title = parsed_page.find('title').text if parsed_page.find('title') else "Без названия"
            logging.info(f"Обработан URL: {target_url}\tЗаголовок: {page_title}")

            extracted_links = []
            for element in parsed_page.find_all(["a", "img", "video", "audio"]):
                href_or_src = element.get("href") or element.get("src")

                if not href_or_src or href_or_src.startswith('#') or href_or_src.startswith(':'):
                    continue

                full_url = resolve_url(target_url, href_or_src)
                if urlparse(full_url).netloc == target_domain:
                    extracted_links.append(full_url)
                    logging.info(f"Обнаружено: {element.name}, {href_or_src}")

            return extracted_links


def push_to_mq(data_list):
    """Отправляет список данных в очередь RabbitMQ."""
    credentials = pika.PlainCredentials(MQ_USER, MQ_PASS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=MQ_HOST, port=MQ_PORT, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=MQ_QUEUE, durable=True)

    for data in data_list:
        channel.basic_publish(exchange='', routing_key=MQ_QUEUE, body=data)
        logging.info(f"Добавлено в очередь: {data}")

    connection.close()


async def execute():
    import sys
    if len(sys.argv) < 2:
        logging.error("Нужен формат: python script.py <URL>")
        return

    input_url = sys.argv[1]

    collected_links = await fetch_content(input_url)
    if collected_links:
        push_to_mq(collected_links)


if __name__ == "__main__":
    asyncio.run(execute())
