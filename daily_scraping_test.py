import asyncio
import redis.asyncio as aioredis
from daily_scraping_fetch_and_parse import scraper, upload_to_s3, get_urls
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Redis Configuration for testing
DB_HOST_LOCAL = os.environ.get("DB_HOST_LOCAL") or "localhost"
DB_PORT_LOCAL = int(os.environ.get("DB_PORT_LOCAL", 6379))
DB_PASS_LOCAL = os.environ.get("DB_PASS_LOCAL")
REDIS_DB_NUMBER_LOCAL = int(os.environ.get("REDIS_DB_NUMBER_LOCAL", 0))
test_source_set = "oreilly_test_set"

# URLs to insert into Redis (loaded from environment variables)
test_urls = os.environ.get("TEST_URLS", "").split(',')

async def insert_urls(redis_client, urls, key):
    for url in urls:
        await redis_client.sadd(key, url)
    print(f"Inserted {len(urls)} URLs into Redis set '{key}'")

def validate_product(product):
    if not isinstance(product.get('retailer'), str) or product['retailer'] is None:
        raise ValueError("Invalid 'retailer': Must be a non-empty string")
    if not isinstance(product.get('product_url'), str) or product['product_url'] is None:
        raise ValueError("Invalid 'product_url': Must be a non-empty string")
    if not isinstance(product.get('retailers_brand'), str) or product['retailers_brand'] is None:
        raise ValueError("Invalid 'retailers_brand': Must be a non-empty string")
    if not isinstance(product.get('price'), (float, int)) or product['price'] is None:
        raise ValueError("Invalid 'price': Must be a non-null number")
    if not isinstance(product.get('title'), str) or product['title'] is None:
        raise ValueError("Invalid 'title': Must be a non-empty string")
    if not isinstance(product.get('retailers_mpn'), str) or product['retailers_mpn'] is None:
        raise ValueError("Invalid 'retailers_mpn': Must be a non-empty string")

async def test_scraper(redis_client, key):
    product_buffer = []
    while True:
        urls = await get_urls(redis_client, key, 1)  # Get one URL at a time
        if not urls:
            break

        for url in urls:
            await scraper(url, product_buffer, test_mode=True)

        if product_buffer:
            for product in product_buffer:
                validate_product(product)
            product_buffer.clear()

async def main():
    redis_client_LOCAL = aioredis.Redis(
        host=DB_HOST_LOCAL,
        port=DB_PORT_LOCAL,
        password=DB_PASS_LOCAL,
        db=REDIS_DB_NUMBER_LOCAL
    )

    try:
        # Insert URLs into Redis
        await insert_urls(redis_client_LOCAL, test_urls, test_source_set)
        # Test scraper one by one
        await test_scraper(redis_client_LOCAL, test_source_set)
    finally:
        await redis_client_LOCAL.close()

if __name__ == "__main__":
    asyncio.run(main())
