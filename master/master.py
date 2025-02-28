import os
import json
import time
import logging
from flask import Flask, request, jsonify
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('master')

app = Flask(__name__)

# Data directory
DATA_DIR = '/data'
os.makedirs(DATA_DIR, exist_ok=True)

# File metadata store : simple implementation 
# On desk to act as checkpointing mechiansim
METADATA_FILE = os.path.join(DATA_DIR, 'metadata.json')

def is_node_registred(node_id, data_nodes):
  """
    Checks weather is node registered or note
  """
  data_node = data_nodes.get(node_id, None)
  if data_node:
    return True, f"Node with id {node_id} is registered!"
  return False, f"Node with id {node_id} is not registered!"

# Initialize metadata
if os.path.exists(METADATA_FILE):
    with open(METADATA_FILE, 'r') as f:
        metadata = json.load(f)
else:
    metadata = {
        'files': {},
        'datanodes': {},
        'chunk_size': 4 * 1024 * 1024  
    }
    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f)

# Registered datanodes
datanodes = {}

def save_metadata():
    """
      All metadata about nodes are stored as one large JSON file this for
      This aims for simplicity, such can be optimized to use SQLITE as storage.
    """
    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f)

@app.route('/register', methods=['POST'])
def register_datanode():
    """
      The following endpiont register datanodes with metadata such:
      url: The url of the datanode. 
      registered_at: Time of registery in the cluster.
      last_heartbeat: The last heart beat

      Then store details
    """
    data = request.json
    node_id = data.get('node_id')
    node_url = data.get('node_url')
    
    # if node exists fail sliently allowing for graceful degradation
    # and robust restarts
    node_exists, message = is_node_registred(node_id, datanodes)
    if node_exists:
        return jsonify({'status': message}), 200

    # if node does not exists we will create it and add it to datanodes
    if not node_id or not node_url:
        return jsonify({'error': 'Missing node_id or node_url'}), 400
    
    datanodes[node_id] = {
        'url': node_url,
        'registered_at': time.time(),
        'last_heartbeat': time.time()
    }
    
    metadata['datanodes'][node_id] = {
        'url': node_url,
        'registered_at': time.time(),
        'last_heartbeat': time.time()
    }
    save_metadata()
    
    logger.info(f"Datanode {node_id} registered at {node_url}")
    return jsonify({'status': 'registered', 'chunk_size': metadata['chunk_size']}), 200


@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    """
      heartbeat and connectivity checks endpoint.
      it expect the node_id and only send heartbeat back if
      the node is connected and configured
    """
    data = request.json
    node_id = data.get('node_id')
    
    if not node_id or node_id not in datanodes:
        return jsonify({'error': 'Unknown node_id'}), 400
    

    datanodes[node_id]['last_heartbeat'] = time.time()
    metadata['datanodes'][node_id]['last_heartbeat'] = time.time()
    save_metadata()
    
    return jsonify({'status': 'alive'}), 200

@app.route('/file', methods=['POST'])
def create_file():
    """
      Expect file with metadata
      1. creates chunks of the file.
      2. distribute them across nodes.
      3. update metadata for checkpointing and presist it.
      4. log the information
      5. return the meta data of the distributed file.
    """
    data = request.json
    filename = data.get('filename')
    filesize = data.get('filesize')
    
    if not filename or not filesize:
        return jsonify({'error': 'Missing filename or filesize'}), 400
    
    # Calculate number of chunks
    chunk_size = metadata['chunk_size']
    num_chunks = (filesize + chunk_size - 1) // chunk_size  
    
    # Get active datanodes
    active_nodes = list(datanodes.keys())
    if not active_nodes:
        return jsonify({'error': 'No active datanodes available'}), 503
    
    # Create file entry in metadata
    file_id = str(int(time.time() * 1000))
    
    chunks = []
    for i in range(num_chunks):
        # Simple round-robin allocation across datanodes
        node_id = active_nodes[i % len(active_nodes)]
        node_url = datanodes[node_id]['url']
        chunk_id = f"{file_id}_{i}"
        
        chunks.append({
            'chunk_id': chunk_id,
            'node_id': node_id,
            'node_url': node_url,
            'start': i * chunk_size,
            'size': min(chunk_size, filesize - i * chunk_size)
        })
    
    metadata['files'][filename] = {
        'file_id': file_id,
        'size': filesize,
        'created_at': time.time(),
        'chunks': chunks
    }
    save_metadata()
    
    logger.info(f"Created file metadata for {filename}, {num_chunks} chunks")
    return jsonify({
        'file_id': file_id,
        'chunks': chunks,
        'chunk_size': chunk_size
    }), 200

@app.route('/file/<filename>', methods=['GET'])
def get_file_info(filename):
    """
      Query file info from the master node.
    """
    if filename not in metadata['files']:
        return jsonify({'error': 'File not found'}), 404
    
    file_info = metadata['files'][filename]
    return jsonify(file_info), 200

@app.route('/files', methods=['GET'])
def list_files():
    """
      get all the files from the master node.
    """
    files = {}
    for filename, file_info in metadata['files'].items():
        files[filename] = {
            'size': file_info['size'],
            'created_at': file_info['created_at']
        }
    return jsonify(files), 200

@app.route('/stats', methods=['POST'])
def record_stats():
    """
      Record DFS status
    """
    data = request.json
    node_id = data.get('node_id')
    operation = data.get('operation')
    bytes_processed = data.get('bytes')
    duration_ms = data.get('duration_ms')
    
    if not node_id or not operation or not bytes_processed or not duration_ms:
        return jsonify({'error': 'Missing required fields'}), 400
    
    # Calculate throughput in MB/s
    throughput = (bytes_processed / 1024 / 1024) / (duration_ms / 1000)
    
    logger.info(f"Node {node_id} - {operation}: {throughput:.2f} MB/s ({bytes_processed} bytes in {duration_ms} ms)")
    return jsonify({'status': 'recorded'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
