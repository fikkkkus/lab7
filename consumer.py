from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import urlparse
import aiohttp
import asyncio
import logging
import os
import pika

# Инициализация параметров из окружения
load_dotenv('params.env')

# Настройка логов
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

# Конфигурация RabbitMQ
RABBIT_HOST = os.getenv("RABBITMQ_HOST")
RABBIT_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBIT_USER = os.getenv("RABBITMQ_USER")
RABBIT_PASS = os.getenv("RABBITMQ_PASSWORD")
RABBIT_QUEUE = os.getenv("QUEUE_NAME")
TIMEOUT_SEC = 30

visited_links = set()

def build_full_url(base, relative):
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

async def parse_page_content(url):
    """Извлекает ссылки и ресурсы с указанного URL."""
    domain = urlparse(url).netloc

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                logging.warning(f"Ошибка при получении страницы: {url}")
                return []

            page_html = await response.text()
            soup = BeautifulSoup(page_html, "html.parser")
            title = soup.title.string if soup.title else "Нет заголовка"
            logging.info(f"Обработан URL: {url}\tЗаголовок: {title}")

            found_links = []
            for element in soup.find_all(["a", "img", "video", "audio"]):
                href_or_src = element.get("href") or element.get("src")

                if not href_or_src or href_or_src.startswith('#') or href_or_src.startswith(':'):
                    continue

                absolute_url = build_full_url(url, href_or_src)

                if absolute_url in visited_links:
                    continue

                if urlparse(absolute_url).netloc == domain:
                    visited_links.add(absolute_url)
                    found_links.append(absolute_url)
                    logging.info(f"Обнаружено: {element.name}, {href_or_src}")

            return found_links

def send_to_rabbitmq(data):
    """Передает данные в очередь RabbitMQ."""
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBIT_HOST, port=RABBIT_PORT, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=RABBIT_QUEUE, durable=True)

    for item in data:
        channel.basic_publish(exchange='', routing_key=RABBIT_QUEUE, body=item)
        logging.info(f"Добавлено в очередь RabbitMQ: {item}")

    connection.close()

async def process_queue():
    """Асинхронная обработка очереди RabbitMQ."""
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBIT_HOST, port=RABBIT_PORT, credentials=credentials))
    try:
        channel = connection.channel()
        channel.queue_declare(queue=RABBIT_QUEUE, durable=True)

        while True:
            method_frame, _, body = channel.basic_get(queue=RABBIT_QUEUE, auto_ack=True)
            if body:
                url = body.decode()
                links = await parse_page_content(url)
                for link in links:
                    channel.basic_publish(exchange='', routing_key=RABBIT_QUEUE, body=link)
                    logging.info(f"Добавлено в очередь RabbitMQ: {link}")
            else:
                await asyncio.sleep(TIMEOUT_SEC)
                break

        logging.info("Очередь обработана. Завершение работы.")
    except Exception as e:
        logging.error(f"Ошибка обработки очереди: {e}")
    finally:
        connection.close()

async def main_task():
    await process_queue()

if __name__ == "__main__":
    try:
        asyncio.run(main_task())
    except KeyboardInterrupt:
        logging.info("Работа прервана пользователем.")
