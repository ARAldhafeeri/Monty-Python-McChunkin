import os
import time
import json
import logging
import requests
import threading
from flask import Flask, request, jsonify, send_file
from io import BytesIO
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Get environment variables
NODE_ID = os.environ.get('NODE_ID', '1')
MASTER_URL = os.environ.get('MASTER_URL', 'http://master:5000')
HOST_PORT = 8000 + int(NODE_ID)  

logger = logging.getLogger(f'datanode-{NODE_ID}')

app = Flask(__name__)

# Data directory
DATA_DIR = '/data'
os.makedirs(DATA_DIR, exist_ok=True)

# Node URL
NODE_URL = f"http://datanode{NODE_ID}:{HOST_PORT}"

# Metrics for throughput
metrics = {
    'reads': [],
    'writes': []
}

def alive_or_dead(url):
  """
    Simple way to check if a node  or master is alive or not
  """
  r = requests.head(url)
  return r.status_code == 200

def register_with_master():
    """
      Check if the current node registered with master node
    """
    try:
        response = requests.post(f"{MASTER_URL}/register", json={
            'node_id': NODE_ID,
            'node_url': NODE_URL
        })
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Registered with master. Chunk size: {data.get('chunk_size')} bytes")
            return True
        else:
            logger.error(f"Failed to register with master: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error registering with master: {str(e)}")
        return False

def send_heartbeat():
    """
      reverse heartbeat implementation, checks if master up.
      then request heartbeat from master
      this allow for two ways heartbeat
    """
    while True:
        try:
            # check is master up
            alive = alive_or_dead(MASTER_URL)
            if not alive:
              logger.debug("Master is dead")

            response = requests.post(f"{MASTER_URL}/heartbeat", json={
                'node_id': NODE_ID
            })
            if response.status_code == 200:
                logger.debug("Heartbeat sent to master")
            else:
                logger.warning(f"Failed to send heartbeat: {response.text}")
        except Exception as e:
            logger.error(f"Error sending heartbeat: {str(e)}")
        
        time.sleep(10)

def report_metric(operation, bytes_processed, duration_ms):
    try:
        requests.post(f"{MASTER_URL}/stats", json={
            'node_id': NODE_ID,
            'operation': operation,
            'bytes': bytes_processed,
            'duration_ms': duration_ms
        })
    except Exception as e:
        logger.error(f"Error reporting metrics: {str(e)}")

@app.route('/chunk/<chunk_id>', methods=['PUT'])
def store_chunk(chunk_id):
    """
      upsert create or edit a chunk within the DFS
    """
    start_time = time.time()
    
    # Get the chunk data
    chunk_data = request.data
    chunk_size = len(chunk_data)
    
    # Store the chunk
    chunk_path = os.path.join(DATA_DIR, chunk_id)
    with open(chunk_path, 'wb') as f:
        f.write(chunk_data)
    
    end_time = time.time()
    duration_ms = (end_time - start_time) * 1000
    
    # Log throughput and report metrics
    throughput = (chunk_size / 1024 / 1024) / (duration_ms / 1000)  # MB/s
    logger.info(f"Stored chunk {chunk_id} ({chunk_size} bytes) at {throughput:.2f} MB/s")
    
    metrics['writes'].append({
        'chunk_id': chunk_id,
        'size': chunk_size,
        'duration_ms': duration_ms,
        'throughput': throughput,
        'timestamp': time.time()
    })
    
    # Report metrics to master
    report_metric('write', chunk_size, duration_ms)
    
    return jsonify({
        'status': 'stored',
        'chunk_id': chunk_id,
        'size': chunk_size,
        'node_id': NODE_ID
    }), 200

@app.route('/chunk/<chunk_id>', methods=['GET'])
def retrieve_chunk(chunk_id):
    """
      reterive a chunk within the DFS
    """
    start_time = time.time()
    
    # Get the chunk
    chunk_path = os.path.join(DATA_DIR, chunk_id)
    
    if not os.path.exists(chunk_path):
        return jsonify({'error': 'Chunk not found'}), 404
    
    with open(chunk_path, 'rb') as f:
        chunk_data = f.read()
    
    end_time = time.time()
    duration_ms = (end_time - start_time) * 1000
    chunk_size = len(chunk_data)
    
    # Log throughput and report metrics
    throughput = (chunk_size / 1024 / 1024) / (duration_ms / 1000)  # MB/s
    logger.info(f"Retrieved chunk {chunk_id} ({chunk_size} bytes) at {throughput:.2f} MB/s")
    
    metrics['reads'].append({
        'chunk_id': chunk_id,
        'size': chunk_size,
        'duration_ms': duration_ms,
        'throughput': throughput,
        'timestamp': time.time()
    })
    
    # Report metrics to master
    report_metric('read', chunk_size, duration_ms)
    
    return send_file(
        BytesIO(chunk_data),
        mimetype='application/octet-stream'
    )

@app.route('/metrics', methods=['GET'])
def get_metrics():
    """
      return metrices upon request 
    """
    return jsonify(metrics), 200

if __name__ == '__main__':
    if register_with_master():
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=send_heartbeat)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()
        
        app.run(host='0.0.0.0', port=HOST_PORT)
    else:
        logger.error("Failed to register with master. Exiting.")