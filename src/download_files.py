# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "requests>=2.31.0",
#   "tqdm>=4.66.0",
#   "pathlib>=1.0.1"
# ]
# ///

import requests
import gzip
from pathlib import Path
from tqdm import tqdm
from typing import List, Union

def download_files(urls: Union[str, List[str]], 
                  skip_existing: bool = True,
                  chunk_size: int = 8192) -> None:
    """
    Download large .gz files to the /data directory relative to the repository root.
    
    Args:
        urls (Union[str, List[str]]): Single URL or list of URLs to download
        skip_existing (bool): If True, skip files that already exist
        chunk_size (int): Size of chunks to download at a time
    
    Raises:
        Exception: If there's an error during download or if data directory cannot be created
    """
    # Convert single URL to list
    if isinstance(urls, str):
        urls = [urls]
    
    # Get the repository root directory (assumes script is in /src)
    repo_root = Path(__file__).parent.parent
    data_dir = repo_root / 'data'
    
    # Create data directory if it doesn't exist
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise Exception(f"Failed to create data directory: {e}")
    
    for url in urls:
        # Extract filename from URL
        filename = url.split('/')[-1]
        filepath = data_dir / filename
        
        # Skip if file exists and skip_existing is True
        if filepath.exists() and skip_existing:
            print(f"Skipping {filename} - file already exists")
            continue
        
        try:
            # Stream the response to handle large files
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Get total file size for progress bar
            total_size = int(response.headers.get('content-length', 0))
            
            print(f"Downloading {filename}")
            
            # Create progress bar
            progress = tqdm(
                total=total_size,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024
            )
            
            # Download and write the file in chunks
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    size = f.write(chunk)
                    progress.update(size)
            
            progress.close()
            print(f"Successfully downloaded {filename}")
            
        except Exception as e:
            print(f"Error downloading {filename}: {e}")
            # Remove partially downloaded file
            if filepath.exists():
                filepath.unlink()
            continue

if __name__ == "__main__":
    urls = [
        "https://downloads.marginalia.nu/exports/urls-meta-23-11-02.csv.gz",
        "https://downloads.marginalia.nu/exports/feeds.csv"
    ]
    download_files(urls)