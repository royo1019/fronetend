from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import pickle
import pandas as pd
from datetime import datetime
from create_model import RuleBasedStalenessDetector
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Load the ML model
MODEL_PATH = 'staleness_detector_model.pkl'
model = None

def load_model():
    """Load the pickle model"""
    global model
    try:
        with open(MODEL_PATH, 'rb') as f:
            model = pickle.load(f)
        logger.info(f"Model loaded successfully from {MODEL_PATH}")
        return True
    except Exception as e:
        logger.error(f"Failed to load model: {str(e)}")
        return False

# Load model on startup
load_model()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'CMDB Analyzer Backend',
        'model_loaded': model is not None
    })

@app.route('/test-connection', methods=['POST'])
def test_connection():
    """Test ServiceNow connection endpoint"""
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
    Scan and analyze CIs for stale ownership using ML model
    """
    if model is None:
        return jsonify({
            'error': 'ML model not loaded. Please check server logs.'
        }), 500

    try:
        data = request.get_json()
        logger.info(f"scan_stale_ownership input: {data}")
        instance_url = data.get('instance_url')
        username = data.get('username')
        password = data.get('password')

        # Fetch data from ServiceNow
        logger.info("Fetching data from ServiceNow...")
        
        # Get CI data
        ci_data = fetch_ci_data(instance_url, username, password, limit=100000000)
        logger.info(f"Fetched {len(ci_data)} CI records")
        if not ci_data:
            logger.error("No CI data fetched")
            return jsonify({'error': 'Failed to fetch CI data'}), 500

        # Get audit data
        audit_data = fetch_audit_data(instance_url, username, password, limit=200000000)
        logger.info(f"Fetched {len(audit_data)} audit records")
        if not audit_data:
            logger.error("No audit data fetched")
            return jsonify({'error': 'Failed to fetch audit data'}), 500

        # Get user data
        user_data = fetch_user_data(instance_url, username, password, limit=500000)
        logger.info(f"Fetched {len(user_data)} user records")
        if not user_data:
            logger.error("No user data fetched")
            return jsonify({'error': 'Failed to fetch user data'}), 500

        logger.info(f"Fetched {len(ci_data)} CIs, {len(audit_data)} audit records, {len(user_data)} users")

        # Process data and make predictions
        try:
            stale_ci_list = analyze_cis_with_model(ci_data, audit_data, user_data)
        except Exception as model_exc:
            logger.error(f"Error in analyze_cis_with_model: {str(model_exc)}", exc_info=True)
            return jsonify({
                "error": f"Model analysis failed: {str(model_exc)}"
            }), 500

        return jsonify({
            'success': True,
            'message': 'Analysis completed successfully',
            'summary': {
                'total_cis_analyzed': len(ci_data),
                'stale_cis_found': len(stale_ci_list),
                'high_confidence_predictions': sum(1 for ci in stale_ci_list if ci['confidence'] > 0.8),
                'critical_risk': sum(1 for ci in stale_ci_list if ci['risk_level'] == 'Critical'),
                'high_risk': sum(1 for ci in stale_ci_list if ci['risk_level'] == 'High')
            },
            'stale_cis': stale_ci_list
        })

    except Exception as e:
        logger.error(f"Error in scan_stale_ownership: {str(e)}", exc_info=True)
        return jsonify({
            "error": f"Scan failed: {str(e)}"
        }), 500

def fetch_ci_data(instance_url, username, password, limit=10000):
    """Fetch CI data from ServiceNow"""
    try:
        url = f"{instance_url}/api/now/table/cmdb_ci"
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        response = requests.get(
            url,
            auth=(username, password),
            headers=headers,
            params={
                'sysparm_limit': limit,
                'sysparm_fields': 'sys_id,name,short_description,sys_class_name,sys_updated_on,assigned_to,assigned_to.user_name'
            },
            timeout=60
        )
        
        if response.status_code == 200:
            return response.json().get('result', [])
        else:
            logger.error(f"Failed to fetch CI data: {response.status_code}")
            return []
            
    except Exception as e:
        logger.error(f"Error fetching CI data: {str(e)}")
        return []

def fetch_audit_data(instance_url, username, password, limit=20000):
    """Fetch audit data from ServiceNow"""
    try:
        url = f"{instance_url}/api/now/table/sys_audit"
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        # Get recent audit records
        response = requests.get(
            url,
            auth=(username, password),
            headers=headers,
            params={
                'sysparm_limit': limit,
                'sysparm_fields': 'sys_created_on,tablename,fieldname,documentkey,user,oldvalue,newvalue',
            },
            timeout=120
        )
        
        if response.status_code == 200:
            return response.json().get('result', [])
        else:
            logger.error(f"Failed to fetch audit data: {response.status_code}")
            return []
            
    except Exception as e:
        logger.error(f"Error fetching audit data: {str(e)}")
        return []

def fetch_user_data(instance_url, username, password, limit=5000):
    """Fetch user data from ServiceNow"""
    try:
        url = f"{instance_url}/api/now/table/sys_user"
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        response = requests.get(
            url,
            auth=(username, password),
            headers=headers,
            params={
                'sysparm_limit': limit,
                'sysparm_fields': 'user_name,name,email,active,sys_created_on,department'
            },
            timeout=60
        )
        
        if response.status_code == 200:
            return response.json().get('result', [])
        else:
            logger.error(f"Failed to fetch user data: {response.status_code}")
            return []
            
    except Exception as e:
        logger.error(f"Error fetching user data: {str(e)}")
        return []

def analyze_cis_with_model(ci_data, audit_data, user_data):
    """Analyze CIs using the ML model and return stale CI list"""
    
    # Convert to pandas DataFrames
    ci_df = pd.DataFrame(ci_data)
    audit_df = pd.DataFrame(audit_data)
    user_df = pd.DataFrame(user_data)
    
    # Log a sample CI for debugging
    if len(ci_data) > 0:
        logger.info(f"Sample CI: {ci_data[0]}")
    
    # Log sample audit data
    if len(audit_data) > 0:
        logger.info(f"Sample audit record: {audit_data[0]}")
        
    # Log sample user data
    if len(user_data) > 0:
        logger.info(f"Sample user: {user_data[0]}")
    
    # Create labels DataFrame from CI data
    labels_data = []
    for ci in ci_data:
        assigned_to = ci.get('assigned_to', None)
        assigned_owner = ''
        if isinstance(assigned_to, dict):
            # Try user_name, then value, then name
            assigned_owner = assigned_to.get('user_name') or assigned_to.get('value') or assigned_to.get('name', '')
        elif assigned_to:
            assigned_owner = str(assigned_to)
        if assigned_owner:
            labels_data.append({
                'ci_id': ci.get('sys_id'),
                'assigned_owner': assigned_owner
            })
    labels_df = pd.DataFrame(labels_data)
    
    logger.info(f"CIs with assigned owners: {len(labels_df)} out of {len(ci_data)}")
    if len(labels_df) > 0:
        logger.info(f"Sample assigned owner: {labels_df.iloc[0].to_dict()}")
    
    if len(labels_df) == 0:
        logger.warning("No CIs with assigned owners found")
        return []
    
    # Convert dates in audit data
    if len(audit_df) > 0:
        audit_df['sys_created_on'] = pd.to_datetime(audit_df['sys_created_on'], errors='coerce')
        logger.info(f"Audit records date range: {audit_df['sys_created_on'].min()} to {audit_df['sys_created_on'].max()}")
    
    logger.info(f"Analyzing {len(labels_df)} CIs with assigned owners...")
    
    # Get stale CI list from model
    stale_ci_list = model.get_stale_ci_list(labels_df, audit_df, user_df, ci_df)
    
    logger.info(f"Found {len(stale_ci_list)} stale CIs")
    
    # If no stale CIs found, let's debug the first few CIs
    if len(stale_ci_list) == 0 and len(labels_df) > 0:
        logger.info("No stale CIs found. Debugging first CI...")
        first_ci = labels_df.iloc[0]
        ci_id = first_ci['ci_id']
        assigned_owner = first_ci['assigned_owner']
        
        # Get audit records for this CI
        ci_audit_records = audit_df[audit_df['documentkey'] == ci_id] if 'documentkey' in audit_df.columns else pd.DataFrame()
        logger.info(f"First CI {ci_id} has {len(ci_audit_records)} audit records")
        
        # Get user info
        user_info = user_df[user_df['user_name'] == assigned_owner].iloc[0].to_dict() if len(user_df[user_df['user_name'] == assigned_owner]) > 0 else {}
        logger.info(f"User info for {assigned_owner}: {user_info}")
        
        # Test model prediction manually
        ci_info = ci_df[ci_df['sys_id'] == ci_id].iloc[0].to_dict() if len(ci_df[ci_df['sys_id'] == ci_id]) > 0 else {}
        test_ci_data = {
            'ci_info': ci_info,
            'audit_records': ci_audit_records.to_dict('records'),
            'user_info': user_info,
            'assigned_owner': assigned_owner
        }
        
        test_result = model.predict_single(test_ci_data)
        logger.info(f"Test prediction for first CI: {test_result}")
    
    return stale_ci_list

@app.route('/reload-model', methods=['POST'])
def reload_model():
    """Reload the ML model"""
    if load_model():
        return jsonify({
            'success': True,
            'message': 'Model reloaded successfully'
        })
    else:
        return jsonify({
            'success': False,
            'error': 'Failed to reload model'
        }), 500

if __name__ == '__main__':
    print("Starting CMDB Analyzer Backend with ML Model...")
    print("Backend will be available at: http://localhost:5000")
    print("Health check: http://localhost:5000/health")
    print("Test connection: POST /test-connection")
    print("Scan for stale ownership: POST /scan-stale-ownership")
    app.run(debug=True, host='0.0.0.0', port=5000)