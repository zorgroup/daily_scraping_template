import random
from parsel import Selector
from datetime import datetime, timezone
import hashlib
import json
import uuid
import boto3
from curl_cffi.requests import AsyncSession
import os
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.chrome.options import Options
import asyncio
import random
# Load environment variables
load_dotenv()

# AWS S3 Configuration
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
bucket_name = os.environ.get("BUCKET_NAME")

# Configuration
retailer = "oreilly"
BULK_SIZE = 100
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

# Function to validate the product JSON
# Function to validate the product JSON
def validate_product(product):
    try:
        if not isinstance(product.get('retailer'), str) or not product['retailer'].strip():
            raise ValueError("Invalid 'retailer': Must be a non-empty string")
        if not isinstance(product.get('product_url'), str) or not product['product_url'].strip():
            raise ValueError("Invalid 'product_url': Must be a non-empty string")
        if not isinstance(product.get('retailers_brand'), str) or not product['retailers_brand'].strip():
            raise ValueError("Invalid 'retailers_brand': Must be a non-empty string")
        if not isinstance(product.get('title'), str) or not product['title'].strip():
            raise ValueError("Invalid 'title': Must be a non-empty string")
        if not isinstance(product.get('retailers_mpn'), str) or not product['retailers_mpn'].strip():
            raise ValueError("Invalid 'retailers_mpn': Must be a non-empty string")
        
        # Convert price to a float if it's a string with commas
        price = product.get('price')
        if isinstance(price, str):
            price = price.replace(',', '').strip()
            try:
                price = float(price)
            except ValueError:
                raise ValueError("Invalid 'price': Must be a valid number")
        elif not isinstance(price, (float, int)) or price is None:
            raise ValueError("Invalid 'price': Must be a non-null number")
        
        product['price'] = price
    except ValueError as e:
        print(f"Validation failed: {e}")
        return False
    return True

# Function to scrape a web page and write data to S3 asynchronously
async def scraper(url, product_buffer, proxies=None, test_mode=False):
    proxy = TEST_PROXY if test_mode else (random.choice(proxies) if proxies else None)
    print(proxy)
    html = await fetch_page(url, proxy)
    if html:
        product = parse_page(url, html)
        print(product)
        if test_mode:
            product_buffer.append(product)
            if len(product_buffer) >= BULK_SIZE:
                product_buffer.clear()
        else:
            if validate_product(product):
                product_buffer.append(product)
                if len(product_buffer) >= BULK_SIZE:
                    upload_to_s3(product_buffer)
                    product_buffer.clear()
            else:
                print(f"\n\n    Failed product: {product}\n\n   ")



async def get_urls(redis_client, key, count):
    urls = await redis_client.spop(key, count)
    return [url.decode('utf-8') for url in urls] if urls else []

# Function to load proxies from Redis
async def load_proxies(redis_client):
    try:
        proxies = await redis_client.smembers(os.environ.get("PROXY_REMOTE_REDIS_KEY"))
        return [proxy.decode('utf-8') for proxy in proxies]
    except Exception as e:
        print(f"Error loading proxies: {e}")
        return []
