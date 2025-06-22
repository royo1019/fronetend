from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'CMDB Analyzer Backend'
    })

@app.route('/test-connection', methods=['POST'])
def test_connection():
    """
    Test ServiceNow connection endpoint
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['instance_url', 'username', 'password']
        for field in required_fields:
            if not data.get(field):
                return jsonify({
                    'error': f'Missing required field: {field}'
                }), 400
        
        instance_url = data['instance_url'].rstrip('/')
        username = data['username']
        password = data['password']
        
        # Test ServiceNow connection
        logger.info(f"Testing connection to {instance_url}")
        
        # Simple connection test - get user info
        test_url = f"{instance_url}/api/now/table/sys_user"
        
        try:
            response = requests.get(
                test_url,
                auth=(username, password),
                headers={
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                params={'sysparm_limit': '1'},
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info("ServiceNow connection successful")
                return jsonify({
                    'success': True,
                    'message': 'Connection successful!',
                    'timestamp': datetime.now().isoformat()
                })
                
            elif response.status_code == 401:
                return jsonify({
                    'success': False,
                    'error': 'Authentication failed. Please check your credentials.'
                }), 401
                
            elif response.status_code == 403:
                return jsonify({
                    'success': False,
                    'error': 'Access denied. User may not have required permissions.'
                }), 403
                
            else:
                return jsonify({
                    'success': False,
                    'error': f'ServiceNow API error: {response.status_code}'
                }), 400
                
        except requests.exceptions.Timeout:
            return jsonify({
                'success': False,
                'error': 'Connection timeout. Please check the instance URL and try again.'
            }), 408
            
        except requests.exceptions.ConnectionError:
            return jsonify({
                'success': False,
                'error': 'Cannot connect to ServiceNow instance. Please verify the URL.'
            }), 503
            
        except requests.exceptions.RequestException as e:
            return jsonify({
                'success': False,
                'error': f'Request failed: {str(e)}'
            }), 500
    
    except Exception as e:
        logger.error(f"Unexpected error in test connection: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Internal server error occurred'
        }), 500

@app.route('/scan-stale-ownership', methods=['POST'])
def scan_stale_ownership():
    """
    Scan sys_audit, cmdb_ci, and sys_user tables to get record counts and field names.
    """
    try:
        data = request.get_json()
        instance_url = data.get('instance_url')
        username = data.get('username')
        password = data.get('password')

        tables = ['sys_audit', 'cmdb_ci', 'sys_user']
        results = {}
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

        # Define fields to fetch for each table
        table_fields = {
            'sys_user': ['user_name', 'name', 'email', 'active', 'sys_created_on', 'department'],
            'cmdb_ci': ['sys_id', 'name', 'short_description', 'sys_class_name', 'sys_updated_on', 'assigned_to'],
            'sys_audit': ['sys_created_on', 'tablename', 'fieldname', 'documentkey', 'record_checkpoint', 'user', 'oldvalue', 'newvalue', 'sys_created_by']
        }

        for table in tables:
            url = f"{instance_url}/api/now/table/{table}"
            fields = table_fields[table]
            # Get sample records with only specified fields
            try:
                response = requests.get(
                    url,
                    auth=(username, password),
                    headers=headers,
                    params={
                        'sysparm_limit': 5,
                        'sysparm_fields': ','.join(fields)
                    },
                    timeout=30
                )
                if response.status_code == 200:
                    data = response.json()
                    sample_records = data.get('result', [])
                    field_names = fields
                else:
                    sample_records = []
                    field_names = fields
            except Exception as e:
                logger.error(f"Error fetching sample records for {table}: {str(e)}")
                sample_records = []
                field_names = fields

            # Get record count
            try:
                count_response = requests.get(
                    url,
                    auth=(username, password),
                    headers=headers,
                    params={'sysparm_count': 'true'},
                    timeout=30
                )
                if count_response.status_code == 200:
                    record_count = int(count_response.headers.get('X-Total-Count', 0))
                else:
                    record_count = 0
            except Exception as e:
                logger.error(f"Error fetching count for {table}: {str(e)}")
                record_count = 0

            results[table] = {
                'record_count': record_count,
                'field_names': field_names,
                'sample_records': sample_records
            }

        return jsonify({
            'tables': results,
            'message': 'Scan completed successfully.'
        }), 200

    except Exception as e:
        logger.error(f"Error in scan_stale_ownership: {str(e)}")
        return jsonify({
            "error": "Scan failed due to server error."
        }), 500

if __name__ == '__main__':
    print("Starting CMDB Analyzer Backend...")
    print("Backend will be available at: http://localhost:5000")
    print("Health check: http://localhost:5000/health")
    print("Test connection: POST /test-connection")
    app.run(debug=True, host='0.0.0.0', port=5000) 