import asyncio
import redis.asyncio as aioredis
from daily_scraping_fetch_and_parse import scraper, upload_to_s3, get_urls, load_proxies
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Redis Configuration
DB_HOST_LOCAL = os.environ.get("DB_HOST_LOCAL") or "localhost"
DB_PORT_LOCAL = int(os.environ.get("DB_PORT_LOCAL", 6379))
DB_PASS_LOCAL = os.environ.get("DB_PASS_LOCAL")
REDIS_DB_NUMBER_LOCAL = int(os.environ.get("REDIS_DB_NUMBER_LOCAL", 0))
CONCURRENCY = 5  # Number of workers (concurrent tasks)
URLS_PER_BATCH = 100  # Number of URLs to fetch from Redis per batch
batch_size = 500

# Retailer Configuration
retailer = "oreilly"
source_set = f"{retailer}_sitemap_urls_master"
destination_set = f"{retailer}_urls_temp"
scraping_state_key = f"{retailer}_scraping_state"

# Function to check and refill URLs from remote Redis if local Redis is empty and state is outdated
async def refill_urls_from_remote(redis_client_remote, redis_client_local):
    # Check if the local Redis destination_set is empty
    if await redis_client_local.scard(destination_set) == 0:
        # Check the last scraping date
        last_scraping_date_bytes = await redis_client_local.get(scraping_state_key)
        last_scraping_date_str = last_scraping_date_bytes.decode("utf-8") if last_scraping_date_bytes else None
        today_date_str = datetime.now().strftime("%Y-%m-%d")

        # Proceed only if the last scraping date is empty or older than today
        if not last_scraping_date_str or last_scraping_date_str < today_date_str:
            print(f"Refilling URLs for {retailer} from remote Redis to local Redis (last scraping date: {last_scraping_date_str}).")
            try:
                cursor = 0
                while True:
                    cursor, urls = await redis_client_remote.sscan(source_set, cursor=cursor, count=batch_size)
                    if urls:
                        await redis_client_local.sadd(destination_set, *urls)
                    if cursor == 0:
                        break
                print(f"Refilled URLs from remote Redis to local Redis.")
                # Update the scraping state with today's date after a successful refill
                await redis_client_local.set(scraping_state_key, today_date_str)
                print(f"Updated scraping state to today's date: {today_date_str}")
            except Exception as e:
                print(f"Error refilling URLs: {e}")
        else:
            print(f"Skipping refill: {retailer} scraping data is already up-to-date (last scraping date: {last_scraping_date_str}).")
    else:
        print(f"Skipping refill: {destination_set} is not empty.")



async def worker(redis_client, key, proxies=None):
    product_buffer = []
    while True:
        urls = await get_urls(redis_client, key, URLS_PER_BATCH)
        if not urls:
            break

        for url in urls:
            await scraper(url, product_buffer, proxies=proxies)

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
        host=os.environ.get("DB_HOST_REMOTE"),
        port=int(os.environ.get("DB_PORT_REMOTE", 6379)),
        password=os.environ.get("DB_PASS_REMOTE"),
        db=int(os.environ.get("REDIS_DB_NUMBER_REMOTE", 0))
    )

    try:
        proxies = await load_proxies(redis_client_LOCAL) if os.environ.get("PROXY_ENABLED", "false").lower() == "true" else None
        await refill_urls_from_remote(redis_client_REMOTE, redis_client_LOCAL)
        workers = [
            asyncio.create_task(worker(redis_client_LOCAL, destination_set, proxies=proxies))
            for _ in range(CONCURRENCY)
        ]
        await asyncio.gather(*workers)
    finally:
        await redis_client_LOCAL.close()
        await redis_client_REMOTE.close()

if __name__ == "__main__":
    asyncio.run(main())
