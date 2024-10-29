import asyncio
import redis.asyncio as aioredis
import json
import hashlib
from datetime import datetime, timezone
import uuid
import boto3
from parsel import Selector
import os
from dotenv import load_dotenv
from curl_cffi.requests import AsyncSession
import sys
import random

# Configuration
retailer = "oreilly"
source_set = f"{retailer}_sitemap_urls_master"
destination_set = f"{retailer}_urls_temp"
scraping_state_key = f"{retailer}_scraping_state"
batch_size = 500
CONCURRENCY = 5  # Number of workers (concurrent tasks)
URLS_PER_BATCH = 100  # Number of URLs to fetch from Redis per batch
BULK_SIZE = 100  # Number of product JSONs to accumulate before sending to S3

# Load environment variables
load_dotenv()

# Environment mode
ENVIRONMENT = os.environ.get("ENVIRONMENT", "production").lower()

# AWS S3 Configuration
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
bucket_name = os.environ.get("BUCKET_NAME")

# Redis Configuration
DB_HOST_REMOTE = os.environ.get("DB_HOST_REMOTE")
DB_PORT_REMOTE = int(os.environ.get("DB_PORT_REMOTE"))
DB_PASS_REMOTE = os.environ.get("DB_PASS_REMOTE")
REDIS_DB_NUMBER_REMOTE = int(os.environ.get("REDIS_DB_NUMBER_REMOTE"))
DB_HOST_LOCAL = os.environ.get("DB_HOST_LOCAL")
DB_PORT_LOCAL = int(os.environ.get("DB_PORT_LOCAL"))
DB_PASS_LOCAL = os.environ.get("DB_PASS_LOCAL")
REDIS_DB_NUMBER_LOCAL = int(os.environ.get("REDIS_DB_NUMBER_LOCAL"))

# Proxy Configuration
PROXY_ENABLED = os.environ.get("PROXY_ENABLED", "false").lower() == "true"
PROXY_REMOTE_REDIS_KEY = os.environ.get("PROXY_REMOTE_REDIS_KEY", "oreilly_proxies")
TEST_PROXY = os.environ.get("TEST_PROXY")

# Initialize the S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

def upload_to_s3(sanitized_products):
    if not sanitized_products:
        return  # Avoid uploading an empty list

    jsonl_content = "\n".join(json.dumps(product) for product in sanitized_products)
    content_hash = hashlib.md5(jsonl_content.encode('utf-8')).hexdigest()
    now = datetime.now(timezone.utc)
    file_name = f"{retailer}/{now.strftime('%Y/%m/%d')}/{content_hash}-{uuid.uuid4()}.jsonl"

    s3.put_object(Bucket=bucket_name, Key=file_name, Body=jsonl_content)
    print(f"Uploaded {len(sanitized_products)} products to S3 as {file_name}")

# Function to fetch a web page
async def fetch_page(url, proxy=None):
    try:
        async with AsyncSession() as s:
            if proxy:
                print(proxy)
                s.proxies = {"http": proxy, "https": proxy}
            resp = await s.get(url=url, impersonate='chrome')
            if resp.status_code == 200:
                return resp.text
            return None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

# Function to parse a web page
def parse_page(url, html):
    selector = Selector(text=html)
    price = selector.css("strong.pricing_price::text").get()
    price = price.replace('$', '') if price else None
    part_number = selector.css("dd.js-ga-product-line-number::text").get()
    part_name = selector.css("h1.js-ga-product-name::text").get()
    rating = selector.css('div[itemprop="ratingValue"]::text').get()
    review_count = selector.css('meta[itemprop="reviewCount"]::attr(content)').get()
    brand = selector.css("img.pdp-brand::attr(alt)").get()
    image_url = selector.css("img.main-image::attr(src)").get()
    upc = selector.css('span[itemprop="sku"]::text').get().strip() if selector.css('span[itemprop="sku"]::text').get() else None

    data_dict = {
        "retailer": 'oreillyauto.com',
        "product_url": url,
        "retailers_brand": brand,
        "price": float(price) if price else None,
        "title": part_name,
        "images": [image_url] if image_url else [],
        "retailers_mpn": part_number,
        "retailers_upc": [upc] if upc else [],
        "avg_rating": rating,
        "number_of_reviews": int(review_count) if review_count else None
    }
    return data_dict

# Function to process scraping
async def scraper(url, product_buffer, proxies=None, test_mode=False):
    proxy = TEST_PROXY if test_mode else (random.choice(proxies) if proxies else None)
    html = await fetch_page(url, proxy)
    if html:
        product = parse_page(url, html)
        print("Hello world")
        print(product)
        product_buffer.append(product)

        if len(product_buffer) >= BULK_SIZE:
            upload_to_s3(product_buffer)
            product_buffer.clear()

async def get_urls(redis_client, key, count):
    urls = await redis_client.spop(key, count)
    return [url.decode('utf-8') for url in urls] if urls else []

# Function to load proxies from Redis
async def load_proxies(redis_client):
    if not PROXY_ENABLED:
        return []
    try:
        proxies = await redis_client.smembers(PROXY_REMOTE_REDIS_KEY)
        return [proxy.decode('utf-8') for proxy in proxies]
    except Exception as e:
        print(f"Error loading proxies: {e}")
        return []

# Function to refill URLs from remote Redis if local Redis is empty
async def refill_urls_from_remote(redis_client_remote, redis_client_local):
    if await redis_client_local.scard(destination_set) == 0:
        try:
            cursor = 0
            while True:
                cursor, urls = await redis_client_remote.sscan(source_set, cursor=cursor, count=batch_size)
                if urls:
                    await redis_client_local.sadd(destination_set, *urls)
                if cursor == 0:
                    break
            print(f"Refilled URLs from remote Redis to local Redis.")
        except Exception as e:
            print(f"Error refilling URLs: {e}")

# Worker function that processes URLs
async def worker(redis_client, key, test_mode=False, proxies=None):
    product_buffer = []
    urls_processed = 0
    max_urls = 1 if test_mode else float('inf')

    while urls_processed < max_urls:
        remaining_urls = max_urls - urls_processed
        urls_to_fetch = min(URLS_PER_BATCH, remaining_urls)
        urls = await get_urls(redis_client, key, urls_to_fetch)

        if not urls:
            break

        for url in urls:
            await scraper(url, product_buffer, proxies=proxies, test_mode=test_mode)
            urls_processed += 1
            if urls_processed >= max_urls:
                break

    if product_buffer:
        upload_to_s3(product_buffer)

async def main():
    redis_client_LOCAL = aioredis.Redis(
        host=DB_HOST_LOCAL,
        port=DB_PORT_LOCAL,
        password=DB_PASS_LOCAL,
        db=REDIS_DB_NUMBER_LOCAL
    )

    redis_client_REMOTE = aioredis.Redis(
        host=DB_HOST_REMOTE,
        port=DB_PORT_REMOTE,
        password=DB_PASS_REMOTE,
        db=REDIS_DB_NUMBER_REMOTE
    )

    test_mode = ENVIRONMENT == "test"

    try:
        # Load proxies if PROXY_ENABLED is true
        proxies = await load_proxies(redis_client_LOCAL) if PROXY_ENABLED else None

        # Refill URLs from remote Redis to local Redis if needed
        await refill_urls_from_remote(redis_client_REMOTE, redis_client_LOCAL)

        # Start the scraping workers
        workers = [
            asyncio.create_task(worker(redis_client_LOCAL, destination_set, test_mode=test_mode, proxies=proxies))
            for _ in range(CONCURRENCY)
        ]
        await asyncio.gather(*workers)
    finally:
        await redis_client_LOCAL.close()
        await redis_client_REMOTE.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
