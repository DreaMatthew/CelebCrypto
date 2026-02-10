# scrape_content.py
import os
import re
import json
import time
import signal
import traceback
from datetime import datetime, timedelta
from dateutil import parser as dateparser
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, WebDriverException
)
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm
import random

# ------------------ USER CONFIG ------------------
LINKS_FILE = "target_links2.json"     # Read the list of URLs from this JSON file
RESULTS_DIR = "results_content"       # Save scraped content (one JSON per file)
CHROMEDRIVER_PATH = None            # Keep as None
# (Removed VISITED_FILE and SAVE_EVERY)
# -------------------------------------------------

# Create results directory
os.makedirs(RESULTS_DIR, exist_ok=True)

# ⬇️ ========== Critical Change 1: Rewrite "Load Progress" Logic ========== ⬇️
# Scan results_content directory to load scraped URLs from existing JSON files
def load_scraped_urls_from_results(results_dir):
    scraped_urls = set()
    if not os.path.exists(results_dir):
        return scraped_urls
    
    print(f"Scanning {results_dir} to load progress...")
    # Use tqdm to show scanning progress
    file_list = os.listdir(results_dir)
    for filename in tqdm(file_list, desc="Scanning existing files"):
        if filename.endswith(".json"):
            filepath = os.path.join(results_dir, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if 'url' in data and data['url']:
                        scraped_urls.add(data['url'])
            except json.JSONDecodeError:
                print(f"\nWarning: Unable to parse {filename}, skipping.")
            except Exception as e:
                print(f"\nError reading {filename}: {e}")
    return scraped_urls

scraped_urls = load_scraped_urls_from_results(RESULTS_DIR)
print(f"Loaded {len(scraped_urls)} processed links from {RESULTS_DIR}.")
# ⬆️ ========================================================== ⬆️


# --- Scraping Helper Functions (From your original code) ---
def extract_date_from_href(href):
    re_date_in_url = re.compile(r"/(\d{4})/(\d{1,2})/(\d{1,2})/")
    m = re_date_in_url.search(href)
    if m:
        y, mo, d = m.groups()
        try:
            return datetime(int(y), int(mo), int(d))
        except:
            return None
    return None

def parse_article_datetime(driver):
    """
    Attempt to extract publish time from the article page:
    Priority 1: Grab "Published" time (your first structure)
    Priority 2: Grab the unique time (your second structure)
    Priority 3: meta[property="article:published_time"]
    Returns datetime or None
    """
    try:
        # ⬇️ ========== Critical Change: Prioritize your two specific structures ========== ⬇️

        # Find the parent container <div ...> containing the time
        # (This div class "flex-col md:block" is common to both structures)
        parent_div_xpath = "//div[contains(@class, 'flex-col') and contains(@class, 'md:block')]"
        time_divs = driver.find_elements(By.XPATH, parent_div_xpath)
        
        if time_divs:
            time_div = time_divs[0] # Usually there is only one

            # Strategy 1: Prioritize finding <span> containing "Published"
            try:
                published_span = time_div.find_element(By.XPATH, ".//span[contains(text(), 'Published')]")
                text = published_span.text.strip()
                # Clean text, e.g., "Published Jan 2, 2025, 2:00 p.m."
                time_text = re.sub(r'(?i)published:?', '', text).strip()
                if time_text:
                    return dateparser.parse(time_text)
            except NoSuchElementException:
                # Strategy 1 failed (no "Published" text), proceed to Strategy 2
                pass
            except Exception as e:
                print(f"Error parsing Published time: {e}")
                pass # Continue to try other methods

            # Strategy 2: Find unique <span> (your second structure)
            try:
                spans = time_div.find_elements(By.XPATH, ".//span")
                if len(spans) == 1: # Strictly limit to exactly one <span>
                    time_text = spans[0].text.strip()
                    if time_text:
                        return dateparser.parse(time_text)
            except Exception as e:
                print(f"Error parsing unique time span: {e}")
                pass # Continue to try other methods

        # ⬆️ ======================================================= ⬆️


        # Strategy 3: (Original meta tag strategy, as backup)
        # meta tag
        meta = driver.find_elements(By.XPATH, "//meta[@property='article:published_time' or @name='og:article:published_time']")
        if meta:
            content = meta[0].get_attribute("content")
            if content:
                try:
                    return dateparser.parse(content)
                except:
                    pass
        
        # Strategy 4: (Original fuzzy search strategy, as last resort)
        # Find tags containing 'Published'
        els = driver.find_elements(By.XPATH, "//*[contains(text(),'Published') or contains(text(),'published') or contains(text(),'Updated') or contains(text(),'•')]")
        for el in els:
            text = el.text.strip()
            # Common: "Published Apr 18, 2025" or "Apr 18, 2025"
            # Remove 'Published' or 'Updated' words
            txt = re.sub(r'(?i)published:?', '', text)
            txt = re.sub(r'(?i)updated:?', '', txt)
            # Truncate to reasonable length
            txt = txt.split('\n')[0][:80].strip()
            try:
                dt = dateparser.parse(txt, fuzzy=True)
                if dt:
                    return dt
            except:
                continue
    except Exception:
        pass
    return None

def extract_author(driver):
    try:
        el = driver.find_element(By.XPATH, "//*[@id='content']//a[contains(@href,'/author')][1]")
        if el:
            return el.text.strip()
    except:
        pass
    # fallback: search for rel=author meta
    try:
        meta = driver.find_elements(By.XPATH, "//meta[@name='author']")
        if meta:
            return meta[0].get_attribute("content")
    except:
        pass
    return ""

def extract_title(driver):
    try:
        el = driver.find_element(By.XPATH, "//*[@id='content']//h1")
        return el.text.strip()
    except:
        try:
            el = driver.find_element(By.TAG_NAME, "h1")
            return el.text.strip()
        except:
            return ""

def extract_summary(driver):
    """
    Attempt to extract article summary/subtitle
    <h2 class="font-sans text-black text-lg leading-[26px] tracking-normal mb-4">...</h2>
    """
    try:
        # We use XPath to find h2 tags containing these key classes
        # 'leading-[26px]' and 'tracking-normal' are the most distinctive
        xpath = "//h2[contains(@class, 'font-sans') and contains(@class, 'leading-[26px]') and contains(@class, 'tracking-normal')]"
        el = driver.find_element(By.XPATH, xpath)
        if el:
            return el.text.strip()
    except:
        pass # Return empty string if not found
    return ""

def extract_content(driver):
    # collect paragraphs under the main article container
    parts = []
    try:
        # Try several common article-body container paths
        candidates = [
            "//*[@id='content']//div[contains(@class,'article-body')]",
            "//*[@id='content']//div[@data-module-name='article-body']",
            "//*[@id='content']//section//div[contains(@class,'article-body')]",
            "//*[@id='content']//div[contains(@class,'content')]",
            "//*[@id='content']//div[contains(@class,'prose')]"  # fallback
        ]
        for c in candidates:
            els = driver.find_elements(By.XPATH, c)
            if els:
                for el in els:
                    ps = el.find_elements(By.XPATH, ".//p")
                    for p in ps:
                        text = p.text.strip()
                        if text:
                            parts.append(text)
                if parts:
                    break
        # If still empty, try grabbing all <p> (Cautious)
        if not parts:
            ps = driver.find_elements(By.XPATH, "//article//p | //main//p")
            for p in ps:
                t = p.text.strip()
                if t:
                    parts.append(t)
    except Exception:
        pass
    return "\n\n".join(parts).strip()

# ⬇️ ========== Critical Change 2: Delete "save_progress_snapshot" function ========== ⬇️
# (This function is no longer needed)
# ⬆️ ================================================================== ⬆️

def save_article_as_file(article, results_dir):
    """
    Save a single article dictionary as an independent .json file.
    Filename format: YYYY-MM-DD_HHMMSS-Sanitized_Title.json
    """
    dt = article.get("time")
    title = article.get("title", "no_title")

    # 1. Format time string
    time_str = "unknown_date"
    iso_time = "" # Used for saving inside JSON

    if isinstance(dt, datetime):
        time_str = dt.strftime("%Y-%m-%d_%H%M%S")
        iso_time = dt.isoformat()
    elif isinstance(dt, str) and dt:
        iso_time = dt
        try:
            # Attempt to parse from ISO string
            dt_parsed = dateparser.parse(dt)
            time_str = dt_parsed.strftime("%Y-%m-%d_%H%M%S") # Fixed _HM%S
        except:
            # Parse failed, try getting from URL
            dt_from_href = extract_date_from_href(article.get("url"))
            if dt_from_href:
                time_str = dt_from_href.strftime("%Y-%m-%d") # Lower precision
            # else: time_str remains "unknown_date"
    else:
        # Time is None, try getting from URL
        dt_from_href = extract_date_from_href(article.get("url"))
        if dt_from_href:
            time_str = dt_from_href.strftime("%Y-%m-%d")
            iso_time = dt_from_href.isoformat()
        # else: time_str remains "unknown_date", iso_time remains ""

    # 2. Sanitize title for filename
    safe_title = re.sub(r'[\\/*?:"<>|]', "", title) # Remove Windows/Linux illegal characters
    safe_title = re.sub(r'\s+', '_', safe_title)  # Replace all whitespace (spaces, newlines) with underscores
    safe_title = safe_title[:100] # Truncate length to prevent filename from being too long

    saveobj = {
        "title": article.get("title",""),
        "summary": article.get("summary", ""), # Add summary field
        "time": iso_time,
        "author": article.get("author",""),
        "content": article.get("content",""),
        "url": article.get("url","")
    }

    # 4. Construct filename and handle potential conflicts
    filename_base = f"{time_str}-{safe_title}"
    filename = os.path.join(results_dir, f"{filename_base}.json")
    counter = 1
    while os.path.exists(filename):
        # If file exists, add suffix (e.g., ..._1.json, ..._2.json)
        filename = os.path.join(results_dir, f"{filename_base}_{counter}.json")
        counter += 1
        
    # 5. Save file
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(saveobj, f, ensure_ascii=False, indent=2)
        return True # Save successful
    except Exception as e:
        print(f"\nFailed to save file {filename}: {e}")
        return False
# ⬆️ ============================================================================ ⬆️


# --- Graceful Exit Handling ---
# ⬇️ ========== Critical Change 3: Simplify "save_state_and_quit" function ========== ⬇️
def save_state_and_quit(driver=None):
    """
    No longer saves visited list, only responsible for safely closing the driver
    """
    print("\nExiting...")
    if driver:
        try:
            driver.quit()
        except:
            pass
    print("Exited.")
    os._exit(0)
# ⬆️ ================================================================ ⬆️

def _signal_handler(sig, frame):
    save_state_and_quit(driver_global)
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)
# ---

# --- Start Selenium ---
chrome_options = Options()
# Uncomment next line for headless run (headless might affect detection)
# chrome_options.add_argument("--headless=new")
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_argument('--incognito')
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_argument('user-agent=Mozilla5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36')
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

service = Service(CHROMEDRIVER_PATH) if CHROMEDRIVER_PATH else Service()
driver = webdriver.Chrome(service=service, options=chrome_options)
driver.set_window_size(1200, 900)
driver_global = driver
# ---

# ⬇️ ========== Login Logic (From v4) ========== ⬇️
print("Starting browser and visiting Coindesk homepage to upload Cookies...")
driver.get("https://www.coindesk.com")
print("Homepage opened. Adding Cookies...")

try:
    # Your Cookie List (Anonymized)
    # Please replace with your actual cookies if running locally
    cookie1_value = "YOUR_CF_BM_VALUE_HERE"
    session_ck_value = "YOUR_SESSION_0_VALUE_HERE"
    session_ck_value2 = "YOUR_SESSION_1_VALUE_HERE"
    COINDESK_SESSION_value = "YOUR_COINDESK_SESSION_VALUE_HERE"
    COINDESK_PREFERENCES_value = "YOUR_COINDESK_PREFERENCES_VALUE_HERE"
    
    cookie_list = [{"name": "__cf_bm", "value": cookie1_value},
                   {"name": "__session__0", "value": session_ck_value},
                   {"name": "__session__1", "value": session_ck_value2},
                   {"name":"COINDESK_SESSION", "value": COINDESK_SESSION_value},
                   {"name":"COINDESK_PREFERENCES", "value":COINDESK_PREFERENCES_value}]

    for cookie in cookie_list:
        driver.add_cookie(cookie)
    
    driver.refresh()
    print("Cookies added and page refreshed.")

    # Handle Accept Button
    try:
        btn = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[6]/div[2]/div/div[1]/div/div[2]/div/button[1]"))
        )
        btn.click()
        print("Accept button clicked.")
    except Exception as e:
        print("Accept button not found (might have been clicked or does not exist).")

except Exception as e:
    print(f"Error during startup and login: {e}")
    print("Script will exit. Please check if cookies are still valid.")
    driver.quit()
    exit()

print("Login successful, preparing to scrape article content...")
# ⬆️ ============================================================================== ⬆️


# ------------------ Main Flow (Modified to read JSON) ------------------
try:
    print(f"Loading URL list from {LINKS_FILE}...")
    if not os.path.exists(LINKS_FILE):
        print(f"Error: {LINKS_FILE} not found! Script will exit.")
        exit()
        
    with open(LINKS_FILE, 'r', encoding='utf-8') as f:
        all_hrefs = json.load(f)
    
    if not isinstance(all_hrefs, list):
        print(f"Error: Content of {LINKS_FILE} is not a list!")
        exit()
        
    # ⬇️ ========== Critical Change 4: Use "scraped_urls" set to filter ========== ⬇️
    links_to_process = [href for href in all_hrefs if href not in scraped_urls]
    # ⬆️ ================================================================= ⬆️
    
    print(f"Total {len(all_hrefs)} links. Successfully scraped {len(scraped_urls)} links.")
    print(f"Need to process {len(links_to_process)} new links this time.")

    # (Removed logic for "Open BASE_URL" and "Click More stories")

    count = 0
    saved_count = 0
    pbar = tqdm(total=len(links_to_process), desc="Scraping article content")

    # Loop through each link
    for href in links_to_process:
        pbar.update(1)
        
        # (Logic from your original code to scrape individual pages)
        try:
            # ‼️ Important: Open a blank page first, then navigate, helps isolate session
            driver.execute_script("window.open('about:blank', '_blank');")
            driver.switch_to.window(driver.window_handles[-1])
            
            # ⬇️ ========== Critical Change 5: Use driver.get() (Fixed in v4) ========== ⬇️
            driver.get(href)
            # ⬆️ ======================================================================= ⬆️
            
        except Exception as e:
            print(f"\nFailed to open page: {href} ({e})")
            # ⬇️ ========== Critical Change 6: Remove visited.add() ========== ⬇️
            # visited.add(href) # (Removed)
            # ⬆️ =================================================== ⬆️
            if len(driver.window_handles) > 1:
                driver.close() # Close failed tab
                driver.switch_to.window(driver.window_handles[0])
            continue

        # 2. Scrape content
        try:
            # Wait for H1 tag to load
            try:
                wait_short = WebDriverWait(driver, 10)
                wait_short.until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
            except:
                pass # Continue trying to scrape even if timed out

            title = extract_title(driver)
            summary = extract_summary(driver)
            author = extract_author(driver)
            dt = parse_article_datetime(driver)
            if dt is None:
                dt = extract_date_from_href(href) or None

            content = extract_content(driver)
            
            if not title and not content:
                # Page might be 404 or empty
                print(f"\nPage scrape failed (no title and content): {href}")
                raise Exception("Page scrape failed or content is empty")

            article = {
                "title": title,
                "summary": summary, # Add summary
                "time": dt, # Pass datetime object or None
                "author": author,
                "content": content,
                "url": href
            }

            # 3. Save to file
            saved = save_article_as_file(article, RESULTS_DIR)
            if saved:
                saved_count += 1
            
            # ⬇️ ========== Critical Change 7: Remove visited.add() ========== ⬇️
            # visited.add(href) # (Removed)
            # ⬆️ =================================================== ⬆️
            count += 1

            # 4. Close tab
            driver.close()
            driver.switch_to.window(driver.window_handles[0])

        except Exception as e:
            print(f"\nError processing article: {href} ({e})")
            # traceback.print_exc() # Uncomment for debugging
            try:
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
            except:
                # If only one window left, navigate to blank to prevent driver error
                try:
                    driver.get("about:blank")
                except:
                    pass # Driver might have crashed
            # ⬇️ ========== Critical Change 8: Remove visited.add() ========== ⬇️
            # visited.add(href) # (Removed)
            # ⬆️ =================================================== ⬆️
            continue

        # ⬇️ ========== Critical Change 9: Remove "Periodic Save" Logic ========== ⬇️
        # if count > 0 and count % SAVE_EVERY == 0:
        #     save_progress_snapshot(list(visited)) 
        #     pbar.set_postfix({"saved": saved_count})
        # ⬆️ ======================================================= ⬆️

        # Small random wait
        time.sleep(0.5 + random.random()*1.2)

    pbar.close()
    print(f"\nDone. Processed {count} links, saved {saved_count} new articles.")

    # ⬇️ ========== Critical Change 10: Remove final "save_visited" ========== ⬇️
    # (Now only calling save_state_and_quit to close driver)
    save_state_and_quit(driver)
    # ⬆️ ========================================================== ⬆️

except Exception as ex:
    print(f"Crawler encountered unhandled exception: {ex}")
    traceback.print_exc()
    save_state_and_quit(driver)
finally:
    try:
        driver.quit()
    except:
        pass
    print("Script finished.")