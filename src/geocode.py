import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count
import time
from typing import List, Dict, Optional
import logging
from urllib.parse import urlparse
from geocode.geocode import Geocode

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def scrape_about_page(url: str) -> Optional[Dict]:
    """
    Scrapes an about page and extracts its main content.
    
    Args:
        url: The URL of the about page to scrape
        
    Returns:
        Dictionary containing the URL and extracted text, or None if failed
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Remove script and style elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer']):
            element.decompose()
        
        # Extract main content - looking for common content containers
        content = None
        for selector in ['main', 'article', '.content', '#content', '.about', '#about']:
            content = soup.select_one(selector)
            if content:
                break
        
        # If no main content found, use body
        if not content:
            content = soup.body
            
        if content:
            text = ' '.join(content.stripped_strings)
            return {
                'url': url,
                'text': text,
                'domain': urlparse(url).netloc
            }
        return None
        
    except Exception as e:
        logging.error(f"Error scraping {url}: {str(e)}")
        return None

def parallel_scrape(urls: List[str], num_processes: Optional[int] = None) -> List[Dict]:
    """
    Scrapes multiple URLs in parallel using multiprocessing.
    
    Args:
        urls: List of URLs to scrape
        num_processes: Number of processes to use (defaults to CPU count)
        
    Returns:
        List of dictionaries containing scraped data
    """
    if num_processes is None:
        num_processes = cpu_count()
    
    logging.info(f"Starting parallel scraping with {num_processes} processes")
    start_time = time.time()
    
    with Pool(num_processes) as pool:
        results = pool.map(scrape_about_page, urls)
    
    # Filter out None results from failed scrapes
    valid_results = [r for r in results if r is not None]
    
    elapsed_time = time.time() - start_time
    logging.info(f"Scraped {len(valid_results)} pages in {elapsed_time:.2f} seconds")
    
    return valid_results

def process_locations(scraped_data: List[Dict], num_cpus: Optional[int] = None) -> List[Dict]:
    """
    Processes scraped text to extract location information using parallel processing.
    This should be run after scraping due to memory requirements.
    
    Args:
        scraped_data: List of dictionaries containing scraped text
        num_cpus: Number of CPU cores to use for parallel processing (defaults to all available)
        
    Returns:
        List of dictionaries with location information added
    """
    gc = Geocode()
    gc.load()  # load geonames data
    
    # Extract text content for parallel processing
    texts = [item['text'] for item in scraped_data]
    
    try:
        # Process locations in parallel
        all_locations = gc.decode_parallel(texts, num_cpus=num_cpus)
        
        # Merge results back with original data
        results = []
        for item, locations in zip(scraped_data, all_locations):
            item['locations'] = locations
            results.append(item)
            
        return results
        
    except Exception as e:
        logging.error(f"Error in parallel location processing: {str(e)}")
        raise

# Example usage:
if __name__ == "__main__":
    # Example URLs
    urls = [
        "http://example1.com/about",
        "http://example2.com/about",
        # ... more URLs ...
    ]
    
    # First phase: Parallel scraping
    scraped_data = parallel_scrape(urls)
    
    # Second phase: Location processing
    # Note: This should be done after scraping due to memory requirements
    results_with_locations = process_locations(scraped_data)
    
    # Example of accessing results
    for result in results_with_locations:
        print(f"URL: {result['url']}")
        print(f"Locations found: {result['locations']}")
        print("---")