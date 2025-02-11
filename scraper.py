import os
import re
import time
import logging
import requests
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from bs4 import BeautifulSoup
from datetime import date, datetime
# from dotenv import load_dotenv
import dateparser

# ------------------------------------------------------------------------------
# Configure logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/scraping_{date.today()}.log"),
        # logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Load environment variables
# ------------------------------------------------------------------------------
# load_dotenv(override=True)  # Reads .env file in current directory
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# ------------------------------------------------------------------------------
# Moroccan month names map
# ------------------------------------------------------------------------------
# This dictionary replaces common Moroccan month variants
# with standard Arabic month names that dateparser can parse.
MOROCCAN_MONTHS_MAP = {
    "يناير": "يناير",       # January
    "فبراير": "فبراير",      # February
    "مارس": "مارس",          # March
    "أبريل": "أبريل",        # April
    "ماي": "مايو",           # May
    "يونيو": "يونيو",        # June
    "يوليوز": "يوليو",       # July
    "غشت": "أغسطس",          # August
    "شتنبر": "سبتمبر",        # September
    "أكتوبر": "أكتوبر",      # October
    "نونبر": "نوفمبر",       # November
    "دجنبر": "ديسمبر"        # December
}

def normalize_moroccan_months(date_text: str) -> str:
    """
    Replace Moroccan month names in date_text with standard Arabic month names
    so that dateparser can parse them properly.
    """
    if not date_text:
        return date_text
    
    for moroccan_name, standard_name in MOROCCAN_MONTHS_MAP.items():
        date_text = date_text.replace(moroccan_name, standard_name)
    return date_text

# ------------------------------------------------------------------------------
# Helper: extract post ID from URL
# ------------------------------------------------------------------------------
def extract_post_id_from_url(article_url: str) -> str:
    """
    Extract the numeric post ID from a Hespress article URL. 
    For example: 
      https://www.hespress.com/...-66055.html 
    should return "66055".
    """
    # This regex will capture digits between a hyphen and .html
    # Adjust if your URLs have a different pattern.
    match = re.search(r"-([\d]+)\.html$", article_url)
    if match:
        return match.group(1)
    else:
        return ""

# ------------------------------------------------------------------------------
# Database setup
# ------------------------------------------------------------------------------
def get_connection():
    """
    Returns a new psycopg2 connection to the PostgreSQL database using env vars.
    """
    logger.info(f"Connected to database {DB_NAME} at host {DB_HOST}.")
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def create_table():
    """
    Creates the table (hespress_articles) if it doesn't already exist.
    We'll use postid as a UNIQUE key.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS hespress_articles (
        id SERIAL PRIMARY KEY,
        postid TEXT UNIQUE,       -- store extracted post ID, unique
        article_url TEXT UNIQUE,         -- store the original URL for reference
        date TIMESTAMP,           -- parsed date/time
        title TEXT,
        category TEXT,
        date_text_ar TEXT,        -- raw Arabic date
        author TEXT,
        content TEXT,
        featured_image TEXT,
        tags TEXT[],
        created_at TIMESTAMP DEFAULT now()
    );
    """
    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
        logger.info("Table hespress_articles ensured to exist.")
    except Exception as e:
        logger.error(f"Error creating table: {e}", exc_info=True)

def article_exists(postid: str) -> bool:
    """
    Checks if an article with the given post ID already exists in the DB.
    Returns True if it exists, otherwise False.
    """
    if not postid:
        return False  # If there's no valid postid, treat as not found.
    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM hespress_articles WHERE postid = %s LIMIT 1", (postid,))
            return cur.fetchone() is not None
    except Exception as e:
        logger.error(f"Error checking existence for postid={postid}: {e}", exc_info=True)
        return False

def insert_article(article_data):
    """
    Insert a single article into the database.
    Uses ON CONFLICT (postid) to skip duplicates.
    article_data is a dict with keys matching the DB columns.
    """
    insert_query = """
    INSERT INTO hespress_articles (
        postid, article_url, category, title, date_text_ar, date,
        author, content, featured_image, tags
    )
    VALUES (
        %(postid)s, %(article_url)s, %(category)s, %(title)s, %(date_text_ar)s, %(date)s,
        %(author)s, %(content)s, %(featured_image)s, %(tags)s
    )
    ON CONFLICT (postid) DO NOTHING;
    """
    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(insert_query, article_data)
            conn.commit()
        logger.info(f"Inserted article: PostID: {article_data.get('postid')} - Date: {article_data.get('date')}")
    except Exception as e:
        logger.error(f"Error inserting article PostID: {article_data.get('postid')} - Date: {article_data.get('date')}: {e}", exc_info=True)

# ------------------------------------------------------------------------------
# Parsing functions
# ------------------------------------------------------------------------------
def parse_listing_page(page_number):
    """
    Fetches one listing page (the 'ajax_listing' HTML).
    Returns a list of dicts: {postid, article_url, category, title, date_text_ar}.
    """
    url = f"https://www.hespress.com/?action=ajax_listing&paged={page_number}"
    logger.info(f"Fetching listing page: {url}")
    
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(like Gecko) Chrome/86.0.4240.183 Safari/537.36"
        )
    }
    
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"Skipping page {page_number}, status code {resp.status_code}")
            return []
    except requests.RequestException as e:
        logger.error(f"RequestException while fetching page {page_number}: {e}", exc_info=True)
        return []
    
    soup = BeautifulSoup(resp.text, "html.parser")
    articles_divs = soup.find_all("div", class_="card")
    
    results = []
    for div in articles_divs:
        category_span = div.find("span", class_="cat")
        category = category_span.get_text(strip=True) if category_span else None
        
        link_a = div.find("a", class_="stretched-link")
        if not link_a:
            continue
        
        article_url = link_a.get("href", "").strip()
        article_title = link_a.get("title", "").strip()
        
        # Date is in <small class="text-muted time">
        date_el = div.find("small", class_="text-muted time")
        date_text_ar = date_el.get_text(strip=True) if date_el else None
        
        if not article_url:
            continue
        
        # Extract postid from the link:
        postid = extract_post_id_from_url(article_url)
        
        results.append({
            "postid": postid,
            "article_url": article_url,
            "category": category,
            "title": article_title,
            "date_text_ar": date_text_ar
        })
    return results

def parse_article_content(article_url):
    """
    Fetches a full article page and parses:
      - author
      - main content (article body)
      - featured image URL
      - tags
      - date from the <span class="date-post">
    Returns a dict with those fields. (We skip extracting the postid here 
    because we're already extracting it from the URL.)
    """
    logger.info(f"Fetching article: {article_url}")
    
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(like Gecko) Chrome/86.0.4240.183 Safari/537.36"
        )
    }
    try:
        resp = requests.get(article_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"Skipping article {article_url}, status code {resp.status_code}")
            return {}
    except requests.RequestException as e:
        logger.error(f"RequestException while fetching article {article_url}: {e}", exc_info=True)
        return {}
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Main content
    article_content_div = soup.find("div", class_="article-content")
    content_text = ""
    if article_content_div:
        # Remove script/style to clean up text
        for tag in article_content_div(["script", "style"]):
            tag.decompose()
        content_text = article_content_div.get_text(separator="\n", strip=True)
    
    # Author
    author_name = ""
    author_span = soup.find("span", class_="author")
    if author_span and author_span.find("a"):
        author_name = author_span.find("a").get_text(strip=True)
    
    # Some articles place the date here <span class="date-post">...<span>
    date_post_span = soup.find("span", class_="date-post")
    date_text_ar = date_post_span.get_text(strip=True) if date_post_span else ""

    # Featured image URL
    featured_image = ""
    featured_img_div = soup.find("div", class_="post-thumbnail featured-img")
    if featured_img_div and featured_img_div.find("img"):
        featured_image = featured_img_div.find("img").get("src", "")
    
    # Tags
    tags_section = soup.find("section", class_="box-tags")
    tags = []
    if tags_section:
        tag_anchors = tags_section.find_all("a", class_="tag_post_tag")
        for t in tag_anchors:
            tags.append(t.get_text(strip=True))
    
    return {
        "author": author_name,
        "content": content_text,
        "featured_image": featured_image,
        "date_text_ar": date_text_ar,  # raw Arabic date found on the article page
        "tags": tags
    }

def parse_arabic_date(date_text_ar):
    """
    Uses dateparser to parse an Arabic date string into a Python datetime (if possible).
    Also normalizes Moroccan month names before parsing.
    """
    if not date_text_ar:
        return None
    # Normalize Moroccan month names
    normalized_text = normalize_moroccan_months(date_text_ar)
    # Attempt to parse
    parsed_date = dateparser.parse(normalized_text, languages=['ar'])
    return parsed_date

# ------------------------------------------------------------------------------
# Main crawler function
# ------------------------------------------------------------------------------
def scrape_hespress(start_page=40167, end_page=40160):
    """
    Orchestrates the scraping process:
      - Ensures the DB table is created.
      - Loops backward from start_page to end_page.
      - For each listing page, parse summary articles.
      - For each article, check if postid exists, parse details if not, then insert into DB.
    """
    create_table()

    # Track how many articles (links) we fetched across all pages
    total_articles_fetched = 0
    
    # If end_page is larger, we invert the step
    step = -1 if start_page > end_page else 1
    
    for page_num in range(start_page, end_page + step, step):
        logger.info(f"Scraping page {page_num}")
        articles_summaries = parse_listing_page(page_num)
        if not articles_summaries:
            logger.info(f"No articles found on page {page_num}")
            continue

        # Add the number of articles returned by parse_listing_page to the total
        total_articles_fetched += len(articles_summaries)
        
        # Process each article on this page
        for summary in articles_summaries:
            postid = summary["postid"]  # from the URL
            
            # ---------------------------------------------------------
            # Check if postid is already in DB before parsing
            # ---------------------------------------------------------
            if article_exists(postid):
                article_url = summary["article_url"]
                logger.info(f"Skipping article (already in DB): {postid} - {article_url}")
                continue
            
            detail_data = parse_article_content(summary["article_url"])
            
            # Priority for date_text_ar: 
            #   1) full article page if available, 
            #   2) otherwise from the listing page
            date_text_ar = detail_data.get("date_text_ar") or summary["date_text_ar"]
            
            # Parse the Arabic date with dateparser (after normalization)
            date_parsed = parse_arabic_date(date_text_ar)
            
            # Combine data
            article_data = {
                "postid": postid,
                "article_url": summary["article_url"],
                "category": summary["category"],
                "title": summary["title"],
                "date_text_ar": date_text_ar,  # raw Arabic date string
                "date": date_parsed,           # datetime object
                "author": detail_data.get("author", ""),
                "content": detail_data.get("content", ""),
                "featured_image": detail_data.get("featured_image", ""),
                "tags": detail_data.get("tags", []),
            }
            
            insert_article(article_data)
            
            # OPTIONAL: short sleep to be polite
            time.sleep(0.5)
        
        # OPTIONAL: delay between pages
        time.sleep(1)

    logger.info(f"Total articles (links) fetched: {total_articles_fetched}")
    print(f"Total articles (links) fetched: {total_articles_fetched}")

if __name__ == "__main__":
    # Example usage: scrape pages backward from 40167 down to 35000
    logger.info("Starting Hespress scraping...")
    start_time = time.time()
    scrape_hespress(start_page=5, end_page=1)
    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f"Scraping completed in {elapsed_time:.2f} seconds.")
    print(f"Scraping completed in {elapsed_time:.2f} seconds.")
