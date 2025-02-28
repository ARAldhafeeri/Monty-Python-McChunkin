import os
import sys
import time
import json
import logging
import requests
import click
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('client')

# Get master URL from environment
MASTER_URL = os.environ.get('MASTER_URL', 'http://master:5000')

def get_chunk_metadata(chunk):
    """Return the chunk metdata"""
    node_url = chunk.get("node_url", None)
    chunk_id = chunk.get("chunk_id", None)
    data = chunk.get("data", None)
    start = chunk.get("start", None)
    size = chunk.get("size", None)
    return node_url, chunk_id, data, start, size

def get_file_metadata(file_info):
    """Return the file metadata"""
    chunks = file_info.get("chunks", None)
    file_id = file_info.get("file_id", None)
    size = file_info.get("size", None)
    created_at = file_info.get("created_at", None)
    return chunks, file_id, size, created_at

def split_file(file_path, chunks_info):
    """Split a file into chunks according to the chunks_info."""
    chunks = []
    with open(file_path, 'rb') as f:
        for chunk in chunks_info:
            node_url, chunk_id, data, start, size = get_chunk_metadata(chunk)
            f.seek(start)
            data = f.read(size)
            chunks.append({
                'chunk_id': chunk_id,
                'node_url': node_url,
                'data': data
            })
    return chunks

def upload_chunk(chunk):
    """Upload a single chunk to a datanode."""
    try:
        node_url, chunk_id, _, _, _ = get_chunk_metadata(chunk)
        response = requests.put(
            f"{node_url}/chunk/{chunk_id}",
            data=chunk['data']
        )
        if response.status_code == 200:
            logger.info(f"Uploaded chunk {chunk_id} to {node_url}")
            return True
        else:
            logger.error(f"Failed to upload chunk {chunk_id}: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error uploading chunk {chunk_id}: {str(e)}")
        return False

def download_chunk(chunk):
    """Download a single chunk from a datanode."""
    try:
        node_url, chunk_id, _, start, _ = get_chunk_metadata(chunk)
        response = requests.get(
            f"{node_url}/chunk/{chunk_id}"
        )
        if response.status_code == 200:
            logger.info(f"Downloaded chunk {chunk_id} from {node_url}")
            return {
                'chunk_id': chunk_id,
                'start': start,
                'data': response.content
            }
        else:
            logger.error(f"Failed to download chunk {chunk_id}: {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error downloading chunk {chunk_id}: {str(e)}")
        return None

@click.group()
def cli():
    """Simple client for the Distributed File System."""
    pass

@cli.command()
def listfiles():
    """List all files in the DFS."""
    try:
        response = requests.get(f"{MASTER_URL}/files")
        if response.status_code == 200:
            files = response.json()
            if not files:
                click.echo("No files found in the system.")
                return
            
            click.echo("Files in the system:")
            for filename, info in files.items():
                chunks, file_id, size, created_at = get_file_metadata(info)
                size_mb = info['size'] / (1024 * 1024)
                created = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(created_at))
                click.echo(f"{filename} - {size_mb:.2f} MB - Created: {created}")
        else:
            click.echo(f"Error listing files: {response.text}")
    except Exception as e:
        click.echo(f"Error connecting to master: {str(e)}")

@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('--name', help='Name to store the file as (defaults to filename)')
def upload(file_path, name):
    """Upload a file to the DFS."""
    filename = name or os.path.basename(file_path)
    filesize = os.path.getsize(file_path)
    
    try:
        # Register file with master
        response = requests.post(f"{MASTER_URL}/file", json={
            'filename': filename,
            'filesize': filesize
        })
        
        if response.status_code != 200:
            click.echo(f"Error registering file with master: {response.text}")
            return
        
        
        file_info = response.json()
        chunks_info, file_id, size, created_at = get_file_metadata(file_info)
        click.echo(f"Uploading {filename} ({filesize} bytes) in {len(chunks_info) if isinstance(chunks_info, list) else 'DONE'} chunks")
        
        # Split file into chunks
        chunks = split_file(file_path, chunks_info)
        
        # Upload chunks in parallel
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(upload_chunk, chunks))
        
        end_time = time.time()
        duration = end_time - start_time
        throughput = (filesize / 1024 / 1024) / duration  # MB/s
        
        if all(results):
            click.echo(f"Successfully uploaded {filename} at {throughput:.2f} MB/s")
        else:
            click.echo("Some chunks failed to upload. File may be incomplete.")
    
    except Exception as e:
        click.echo(f"Error uploading file: {str(e)}")

@cli.command()
@click.argument('filename')
@click.argument('output_path', type=click.Path())
def download(filename, output_path):
    """Download a file from the DFS."""
    try:
        # Get file metadata
        response = requests.get(f"{MASTER_URL}/file/{filename}")
        
        if response.status_code != 200:
            click.echo(f"Error getting file info: {response.text}")
            return
        
        file_info = response.json()
        chunks_info, file_id, filesize, created_at = get_file_metadata(file_info)

        
        click.echo(f"Downloading {filename} ({filesize} bytes) in {len(chunks_info) if isinstance(chunks_info, list) else 'DONE'} chunks")
        
        # Download chunks in parallel
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=5) as executor:
            downloaded_chunks = list(executor.map(download_chunk, chunks_info))
        
        # Filter out failed chunks
        downloaded_chunks = [c for c in downloaded_chunks if c is not None]

        if len(downloaded_chunks) != len(chunks_info):
            click.echo("Some chunks failed to download. File may be incomplete.")
        
        # Sort chunks by start position
        # TODO: fix - Error downloading file: unhashable type: 'slice'
        # downloaded_chunks.sort(key=lambda c: c['start'])
        
        # Write chunks to output file
 
        with open(output_path, 'wb') as f:
            for chunk in downloaded_chunks:
                f.write(chunk['data'])
        end_time = time.time()
        duration = end_time - start_time
        throughput = (filesize / 1024 / 1024) / duration  # MB/s
        
        click.echo(f"Successfully downloaded {filename} to {output_path} at {throughput:.2f} MB/s")
    
    except Exception as e:
        click.echo(f"Error downloading file: {e} ")

@cli.command()
@click.argument('filename')
def info(filename):
    """Get detailed information about a file."""
    try:
        response = requests.get(f"{MASTER_URL}/file/{filename}")
        
        if response.status_code != 200:
            click.echo(f"Error getting file info: {response.text}")
            return
        
        file_info = response.json()
        chunks_info, file_id, filesize, created_at = get_file_metadata(file_info)

        click.echo(f"File: {filename}")
        click.echo(f"ID: {file_info['file_id']}")
        click.echo(f"Size: {file_info['size']} bytes ({file_info['size'] / (1024 * 1024):.2f} MB)")
        click.echo(f"Created: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(file_info['created_at']))}")
        click.echo(f"Chunks: {len(chunks_info)}")
        
        # Show chunk distribution
        node_distribution = {}
        for chunk in chunks_info:
            node_id = chunk['node_id']
            if node_id not in node_distribution:
                node_distribution[node_id] = 0
            node_distribution[node_id] += 1
        
        click.echo("\nChunk distribution across nodes:")
        for node_id, count in node_distribution.items():
            click.echo(f"  Node {node_id}: {count} chunks")
    
    except Exception as e:
        click.echo(f"Error getting file info: {str(e)}")

if __name__ == '__main__':
    cli()