import duckdb
from typing import List, Generator
from time import time
from datetime import datetime

ABOUT_PATTERNS = [
    # English patterns
    "/about",
    "/about-me",
    "/bio",
    "/biography",
    "/me",
    "/who-am-i",
    "/about/me",
    "/hello",
    "/introduction",
    "/personal",
    "/profile",
    "/my-story",
    "/my-journey",
    "/about-the-author",
    "/meet-me",
    # Spanish patterns
    "/sobre-mi",
    "/quien-soy",
    "/acerca-de-mi",
    "/mi-biografia",
    "/mi-historia",
    "/biografia",
    "/presentacion",
    "/perfil",
    "/conoceme",
    "/hola",
    # German patterns
    "/ueber-mich",
    "/über-mich",
    "/ich",
    "/meine-geschichte",
    "/biografie",
    "/das-bin-ich",
    "/steckbrief",
    "/vorstellung",
    "/hallo",
    "/personliches",
    "/persönliches",
    # French patterns
    "/a-propos",
    "/qui-suis-je",
    "/biographie",
    "/ma-bio",
    "/mon-parcours",
    "/me-connaitre",
    "/me-connaître",
    "/presentation",
    "/présentation",
    "/bonjour",
    "/mon-histoire",
]


# Existing ABOUT_PATTERNS list remains the same

def print_timing(start_time: float, section: str):
    """Print timing information for a section."""
    elapsed = time() - start_time
    print(f"{datetime.now().strftime('%H:%M:%S')} - {section}: {elapsed:.2f} seconds")

def extract_domain(url: str) -> str:
    """Extract domain from URL by removing protocol and path."""
    domain = url.replace('http://', '').replace('https://', '')
    domain = domain.split('/')[0]
    domain = domain.split(':')[0]
    return domain

def process_in_chunks(urls_meta_path: str, feeds_file: str, output_path: str, chunk_size: int = 1000000):
    """Process URLs meta and feeds files in chunks to reduce memory usage."""
    print(f"\n{datetime.now().strftime('%H:%M:%S')} - Processing data files in chunks...")
    start_time = time()

    # Configure DuckDB for memory efficiency
    conn = duckdb.connect(":memory:")
    conn.execute("PRAGMA threads=4")
    conn.execute("PRAGMA memory_limit='4GB'")  # Reduced memory limit
    conn.execute("PRAGMA temp_directory='/tmp'")
    conn.execute("PRAGMA enable_progress_bar=true")
    
    # Register domain extraction function
    conn.create_function('extract_domain', extract_domain, ['VARCHAR'], 'VARCHAR')

    # Create feeds table with domain index
    conn.execute(f"""
        CREATE TABLE feeds AS 
        SELECT dd
            column0 as feed_url,
            extract_domain(column0) as feed_domain
        FROM read_csv_auto('{feeds_file}');
        
        CREATE INDEX idx_feed_domain ON feeds(feed_domain);
    """)

    # Process URLs meta in chunks
    conn.execute(f"""
        CREATE TABLE urls_meta (
            url VARCHAR,
            url_domain VARCHAR,
            title VARCHAR,
            description VARCHAR
        );
    """)

    # Count total rows for progress tracking
    total_rows = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{urls_meta_path}')
    """).fetchone()[0]
    
    processed_rows = 0
    
    # Process in chunks
    while processed_rows < total_rows:
        conn.execute(f"""
            INSERT INTO urls_meta
            SELECT 
                column0 as url,
                extract_domain(column0) as url_domain,
                column2 as title,
                column3 as description
            FROM read_parquet('{urls_meta_path}')
            LIMIT {chunk_size}
            OFFSET {processed_rows};
        """)
        
        processed_rows += chunk_size
        print(f"Processed {min(processed_rows, total_rows):,} / {total_rows:,} rows")
        
    # Create index after loading all data
    conn.execute("CREATE INDEX idx_url_domain ON urls_meta(url_domain)")

    # Perform the join in chunks and write to parquet
    print(f"\n{datetime.now().strftime('%H:%M:%S')} - Performing domain join and writing to parquet...")
    
    conn.execute(f"""
        COPY (
            SELECT 
                f.feed_url,
                f.feed_domain,
                u.url,
                u.url_domain,
                u.title,
                u.description
            FROM feeds f
            LEFT JOIN urls_meta u
                ON position(f.feed_domain IN u.url_domain) > 0
        ) TO '{output_path}'
        (FORMAT PARQUET, COMPRESSION 'ZSTD', CHUNK_SIZE {chunk_size});
    """)

    total_matches = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
    print(f"{datetime.now().strftime('%H:%M:%S')} - Written {total_matches:,} matched records to {output_path}")
    print_timing(start_time, "Join and write")
    
    conn.close()

def filter_about_pages_streaming(input_parquet: str, about_pages: List[str], chunk_size: int = 100000) -> Generator[dict, None, None]:
    """Stream through the joined data to filter about pages with minimal memory usage."""
    print(f"\n{datetime.now().strftime('%H:%M:%S')} - Filtering for about pages (streaming)...")
    start_time = time()
    
    conn = duckdb.connect(":memory:")
    conn.execute("PRAGMA memory_limit='2GB'")
    
    # Create about patterns table
    conn.execute("CREATE TEMPORARY TABLE about_patterns (pattern VARCHAR);")
    for pattern in about_pages:
        conn.execute("INSERT INTO about_patterns VALUES (?)", [pattern])

    # Get total count for progress tracking
    total_rows = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{input_parquet}')
    """).fetchone()[0]
    
    processed_rows = 0
    matching_count = 0
    
    while processed_rows < total_rows:
        results = conn.execute("""
            SELECT DISTINCT 
                feed_url,
                feed_domain,
                url,
                url_domain,
                title,
                description
            FROM read_parquet(?)
            WHERE EXISTS (
                SELECT 1 
                FROM about_patterns p 
                WHERE LOWER(url) LIKE '%' || LOWER(p.pattern) || '%'
            )
            LIMIT ?
            OFFSET ?;
        """, [input_parquet, chunk_size, processed_rows]).fetchall()
        
        for row in results:
            matching_count += 1
            yield {
                "feed_url": row[0],
                "feed_domain": row[1],
                "url": row[2],
                "url_domain": row[3],
                "title": row[4],
                "description": row[5]
            }
        
        processed_rows += chunk_size
        if processed_rows % (chunk_size * 10) == 0:
            print(f"Processed {min(processed_rows, total_rows):,} / {total_rows:,} rows, found {matching_count:,} matches")
    
    print(f"{datetime.now().strftime('%H:%M:%S')} - Found {matching_count:,} about pages total")
    print_timing(start_time, "About page filtering")
    
    conn.close()

def main(urls_meta_path: str, feeds_file: str, output_path: str, about_pages: List[str], chunk_size: int = 1000000) -> List[dict]:
    """Main function to process URLs and find matching about pages with memory optimization."""
    total_start = time()
    print(f"\n{datetime.now().strftime('%H:%M:%S')} - Starting processing...")

    # Process and join data in chunks
    process_in_chunks(urls_meta_path, feeds_file, output_path, chunk_size)
    
    # Stream through results to find about pages
    matching_pages = list(filter_about_pages_streaming(output_path, about_pages))

    print(f"\n{datetime.now().strftime('%H:%M:%S')} - Total processing time: {time() - total_start:.2f} seconds")
    return matching_pages

if __name__ == "__main__":
    urls_meta_path = "../data/urls-meta-23-11-02.parquet"
    feeds_file = "../data/feeds.csv"
    output_path = "../data/domain_joined_data.parquet"

    matching_pages = main(urls_meta_path, feeds_file, output_path, ABOUT_PATTERNS)
    print("\nFirst 10 matching about pages:")
    for page in matching_pages[:10]:
        print(f"\nFeed URL: {page['feed_url']}")
        print(f"URL: {page['url']}")
        if page["title"]:
            print(f"Title: {page['title']}")
        if page["description"]:
            print(f"Description: {page['description']}")