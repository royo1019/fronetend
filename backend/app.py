from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import pickle
import pandas as pd
from datetime import datetime
import sys
import os
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add the current directory to Python path to find the module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from KB_Model import EnhancedStalenessDetector, ScenarioKnowledgeBase
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Load the ML model
MODEL_PATH = 'KB_Model(2).pkl'
model = None

# ServiceNow API configuration
CHUNK_SIZE = 1000  # Number of records to fetch per request
MAX_RETRIES = 3
RETRY_BACKOFF = 1
TIMEOUT = (30, 90)  # (connect timeout, read timeout)

def create_session():
    """Create a requests session with retry logic"""
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def fetch_servicenow_data(endpoint, query_params, auth):
    """
    Fetch data from ServiceNow in chunks with retry logic
    
    Args:
        endpoint (str): API endpoint
        query_params (dict): Query parameters
        auth (tuple): (username, password)
        
    Returns:
        list: List of records
    """
    session = create_session()
    all_records = []
    offset = 0
    
    while True:
        try:
            # Update query params for pagination
            chunk_params = query_params.copy()
            chunk_params['sysparm_offset'] = offset
            chunk_params['sysparm_limit'] = CHUNK_SIZE
            
            response = session.get(
                endpoint,
                params=chunk_params,
                auth=auth,
                timeout=TIMEOUT
            )
            response.raise_for_status()
            
            chunk_data = response.json().get('result', [])
            chunk_size = len(chunk_data)
            
            if chunk_size == 0:
                break
                
            all_records.extend(chunk_data)
            logger.info(f"Fetched {chunk_size} records (offset: {offset})")
            
            if chunk_size < CHUNK_SIZE:
                break
                
            offset += CHUNK_SIZE
            
            # Small delay between chunks to avoid rate limiting
            time.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data (offset {offset}): {str(e)}")
            if offset == 0:  # If first chunk fails, raise error
                raise
            break  # Otherwise, return partial data
            
    return all_records

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
        try:
            # If loading fails, create new model
            logger.info("Attempting to create new model instance")
            model = EnhancedStalenessDetector()
            # Save the model
            with open(MODEL_PATH, 'wb') as f:
                pickle.dump(model, f)
            logger.info(f"Created and saved new model to {MODEL_PATH}")
            return True
        except Exception as e2:
            logger.error(f"Failed to create new model: {str(e2)}")
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
        ci_data = fetch_servicenow_data(f"{instance_url}/api/now/table/cmdb_ci", {}, (username, password))
        logger.info(f"Fetched {len(ci_data)} CI records")
        if not ci_data:
            logger.error("No CI data fetched")
            return jsonify({'error': 'Failed to fetch CI data'}), 500

        # Get audit data
        audit_data = fetch_servicenow_data(f"{instance_url}/api/now/table/sys_audit", {}, (username, password))
        logger.info(f"Fetched {len(audit_data)} audit records")
        if not audit_data:
            logger.error("No audit data fetched")
            return jsonify({'error': 'Failed to fetch audit data'}), 500

        # Get user data
        user_data = fetch_servicenow_data(f"{instance_url}/api/now/table/sys_user", {}, (username, password))
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

        # Group stale CIs by recommended owners
        grouped_by_owners = group_cis_by_recommended_owners(stale_ci_list)

        return jsonify({
            'success': True,
            'message': 'Analysis completed successfully',
            'summary': {
                'total_cis_analyzed': len(ci_data),
                'stale_cis_found': len(stale_ci_list),
                'high_confidence_predictions': sum(1 for ci in stale_ci_list if ci['confidence'] > 0.8),
                'critical_risk': sum(1 for ci in stale_ci_list if ci['risk_level'] == 'Critical'),
                'high_risk': sum(1 for ci in stale_ci_list if ci['risk_level'] == 'High'),
                'recommended_owners_count': len(grouped_by_owners)
            },
            'stale_cis': stale_ci_list,
            'grouped_by_owners': grouped_by_owners
        })

    except Exception as e:
        logger.error(f"Error in scan_stale_ownership: {str(e)}", exc_info=True)
        return jsonify({
            "error": f"Scan failed: {str(e)}"
        }), 500

@app.route('/api/fetch-data', methods=['POST'])
def fetch_data():
    """Fetch CI, audit, and user data from ServiceNow"""
    try:
        # Get credentials from request
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        instance = data.get('instance')
        
        if not all([username, password, instance]):
            return jsonify({'error': 'Missing credentials'}), 400
            
        base_url = f'https://{instance}.service-now.com/api/now/table'
        auth = (username, password)
        
        # Fetch CI data
        logger.info("Fetching CI data...")
        ci_params = {
            'sysparm_fields': 'sys_id,name,short_description,sys_class_name,sys_updated_on,assigned_to,assigned_to.user_name,assigned_to.name'
        }
        ci_data = fetch_servicenow_data(f'{base_url}/cmdb_ci', ci_params, auth)
        logger.info(f"Fetched {len(ci_data)} CI records")
        
        if not ci_data:
            return jsonify({'error': 'No CI data fetched'}), 404
            
        # Fetch audit data for these CIs
        logger.info("Fetching audit data...")
        ci_sys_ids = [ci['sys_id'] for ci in ci_data]
        audit_params = {
            'sysparm_query': f"documentkey IN {','.join(ci_sys_ids)}",
            'sysparm_fields': 'documentkey,sys_created_on,user,fieldname,oldvalue,newvalue'
        }
        audit_data = fetch_servicenow_data(f'{base_url}/sys_audit', audit_params, auth)
        logger.info(f"Fetched {len(audit_data)} audit records")
        
        # Fetch user data
        logger.info("Fetching user data...")
        user_params = {
            'sysparm_fields': 'user_name,name,title,department,location,email,active'
        }
        user_data = fetch_servicenow_data(f'{base_url}/sys_user', user_params, auth)
        logger.info(f"Fetched {len(user_data)} user records")
        
        # Analyze data
        stale_cis = analyze_cis_with_model(ci_data, audit_data, user_data)
        
        return jsonify({
            'ci_count': len(ci_data),
            'audit_count': len(audit_data),
            'user_count': len(user_data),
            'stale_cis': stale_cis
        })
        
    except requests.exceptions.RequestException as e:
        logger.error(f"ServiceNow API error: {str(e)}")
        return jsonify({'error': f'ServiceNow API error: {str(e)}'}), 503
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

def analyze_cis_with_model(ci_data, audit_data, user_data):
    """Analyze CIs using the KB model to detect stale ownership"""
    try:
        stale_ci_list = []
        
        # Convert data to pandas DataFrames
        ci_df = pd.DataFrame(ci_data)
        audit_df = pd.DataFrame(audit_data)
        user_df = pd.DataFrame(user_data)
        
        # Process each CI
        for _, ci in ci_df.iterrows():
            try:
                # Prepare data for single CI prediction
                ci_info = {
                    'ci_data': ci.to_dict(),
                    'audit_data': audit_df[audit_df['documentkey'] == ci['sys_id']].to_dict('records'),
                    'user_data': user_df.to_dict('records')
                }
                
                # Get prediction and details from model
                prediction = model.predict_single(ci_info)
                
                if prediction.get('is_stale', False):
                    # Extract owner information
                    assigned_to = ci.get('assigned_to', {})
                    current_owner = assigned_to.get('user_name', 'Unassigned') if isinstance(assigned_to, dict) else str(assigned_to)
                    owner_display_name = assigned_to.get('name', current_owner) if isinstance(assigned_to, dict) else current_owner
                    
                    stale_ci = {
                        'ci_id': ci['sys_id'],
                        'ci_name': ci['name'],
                        'current_owner': current_owner,
                        'owner_display_name': owner_display_name,
                        'ci_class': ci['sys_class_name'],
                        'last_updated': ci['sys_updated_on'],
                        'confidence': prediction.get('confidence', 0.0),
                        'risk_level': prediction.get('risk_level', 'Medium'),
                        'staleness_reasons': prediction.get('reasons', []),
                        'recommended_actions': prediction.get('recommended_actions', []),
                        'suggested_owners': prediction.get('suggested_owners', []),
                        'staleness_score': prediction.get('staleness_score', 0.0),
                        'evidence_strength': prediction.get('evidence_strength', 'Medium'),
                        'scenario_matches': prediction.get('scenario_matches', [])
                    }
                    stale_ci_list.append(stale_ci)
                    
            except Exception as ci_error:
                logger.warning(f"Error processing CI {ci['sys_id']}: {str(ci_error)}")
                continue
        
        # Sort by staleness score descending
        stale_ci_list.sort(key=lambda x: x['staleness_score'], reverse=True)
        
        # Log analysis results
        logger.info(f"Analysis complete: Found {len(stale_ci_list)} stale CIs out of {len(ci_df)} total CIs")
        if stale_ci_list:
            high_confidence = sum(1 for ci in stale_ci_list if ci['confidence'] > 0.8)
            critical_risk = sum(1 for ci in stale_ci_list if ci['risk_level'] == 'Critical')
            strong_evidence = sum(1 for ci in stale_ci_list if ci['evidence_strength'] == 'Strong')
            logger.info(f"High confidence: {high_confidence}, Critical risk: {critical_risk}, Strong evidence: {strong_evidence}")
        
        return stale_ci_list
        
    except Exception as e:
        logger.error(f"Error in analyze_cis_with_model: {str(e)}", exc_info=True)
        raise

def group_cis_by_recommended_owners(stale_ci_list):
    """
    Group stale CIs by their recommended owners for bulk assignment analysis
    """
    grouped = {}
    
    for ci in stale_ci_list:
        recommended_owners = ci.get('recommended_owners', [])
        
        # If CI has recommended owners, group by the top recommendation
        if recommended_owners and len(recommended_owners) > 0:
            top_recommendation = recommended_owners[0]  # Get the best recommendation
            username = top_recommendation.get('username', 'Unknown')
            
            if username not in grouped:
                grouped[username] = {
                    'recommended_owner': {
                        'username': username,
                        'display_name': top_recommendation.get('display_name', username),
                        'department': top_recommendation.get('department', 'Unknown'),
                        'avg_score': 0,
                        'total_activity_count': 0
                    },
                    'cis_to_assign': [],
                    'total_cis': 0,
                    'risk_breakdown': {
                        'Critical': 0,
                        'High': 0,
                        'Medium': 0,
                        'Low': 0
                    },
                    'avg_confidence': 0
                }
            
            # Add CI to this owner's group
            grouped[username]['cis_to_assign'].append({
                'ci_id': ci.get('ci_id'),
                'ci_name': ci.get('ci_name'),
                'ci_class': ci.get('ci_class'),
                'current_owner': ci.get('current_owner'),
                'confidence': ci.get('confidence'),
                'risk_level': ci.get('risk_level'),
                'staleness_reasons': ci.get('staleness_reasons', [])
            })
            
            # Update aggregated statistics
            grouped[username]['total_cis'] += 1
            grouped[username]['risk_breakdown'][ci.get('risk_level', 'Low')] += 1
            
            # Update averages
            current_total = grouped[username]['total_cis']
            current_avg_confidence = grouped[username]['avg_confidence']
            grouped[username]['avg_confidence'] = (
                (current_avg_confidence * (current_total - 1) + ci.get('confidence', 0)) / current_total
            )
            
            # Update owner stats
            current_avg_score = grouped[username]['recommended_owner']['avg_score']
            grouped[username]['recommended_owner']['avg_score'] = (
                (current_avg_score * (current_total - 1) + top_recommendation.get('score', 0)) / current_total
            )
            grouped[username]['recommended_owner']['total_activity_count'] += top_recommendation.get('activity_count', 0)
        
        else:
            # Handle CIs with no recommendations
            if 'No Recommendation' not in grouped:
                grouped['No Recommendation'] = {
                    'recommended_owner': {
                        'username': 'No Recommendation',
                        'display_name': 'No Suitable Owner Found',
                        'department': 'Manual Review Required',
                        'avg_score': 0,
                        'total_activity_count': 0
                    },
                    'cis_to_assign': [],
                    'total_cis': 0,
                    'risk_breakdown': {
                        'Critical': 0,
                        'High': 0,
                        'Medium': 0,
                        'Low': 0
                    },
                    'avg_confidence': 0
                }
            
            grouped['No Recommendation']['cis_to_assign'].append({
                'ci_id': ci.get('ci_id'),
                'ci_name': ci.get('ci_name'),
                'ci_class': ci.get('ci_class'),
                'current_owner': ci.get('current_owner'),
                'confidence': ci.get('confidence'),
                'risk_level': ci.get('risk_level'),
                'staleness_reasons': ci.get('staleness_reasons', [])
            })
            
            grouped['No Recommendation']['total_cis'] += 1
            grouped['No Recommendation']['risk_breakdown'][ci.get('risk_level', 'Low')] += 1
            
            current_total = grouped['No Recommendation']['total_cis']
            current_avg_confidence = grouped['No Recommendation']['avg_confidence']
            grouped['No Recommendation']['avg_confidence'] = (
                (current_avg_confidence * (current_total - 1) + ci.get('confidence', 0)) / current_total
            )
    
    # Convert to list and sort by total CIs (most CIs first)
    grouped_list = []
    for username, data in grouped.items():
        # Round averages for cleaner display
        data['avg_confidence'] = round(data['avg_confidence'], 2)
        data['recommended_owner']['avg_score'] = round(data['recommended_owner']['avg_score'], 1)
        
        grouped_list.append({
            'username': username,
            **data
        })
    
    # Sort by total CIs descending (owners with most CIs first)
    grouped_list.sort(key=lambda x: x['total_cis'], reverse=True)
    
    return grouped_list

@app.route('/reload-model', methods=['POST'])
def reload_model():
    """Reload the ML model"""
    try:
        global model
        with open('KB_Model(2).pkl', 'rb') as f:
            model = pickle.load(f)
        return jsonify({'success': True, 'message': 'Model reloaded successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/assign-ci-owner', methods=['POST'])
def assign_ci_owner():
    """Assign a CI to a new owner by updating the assigned_to field"""
    try:
        data = request.get_json()
        instance_url = data.get('instance_url')
        username = data.get('username')
        password = data.get('password')
        ci_id = data.get('ci_id')
        new_owner_username = data.get('new_owner_username')
        
        if not all([instance_url, username, password, ci_id, new_owner_username]):
            return jsonify({'error': 'Missing required parameters'}), 400
        
        # First, get the user's sys_id from the username
        user_url = f"{instance_url}/api/now/table/sys_user"
        user_response = requests.get(
            user_url,
            auth=(username, password),
            headers={'Accept': 'application/json'},
            params={'sysparm_query': f'user_name={new_owner_username}', 'sysparm_limit': 1},
            timeout=30
        )
        
        if user_response.status_code != 200:
            return jsonify({'error': 'Failed to find user in ServiceNow'}), 500
        
        user_data = user_response.json().get('result', [])
        if not user_data:
            return jsonify({'error': f'User {new_owner_username} not found in ServiceNow'}), 404
        
        user_sys_id = user_data[0].get('sys_id')
        user_display_name = user_data[0].get('name', new_owner_username)
        
        # Update the CI's assigned_to field with the user's name (not sys_id)
        ci_url = f"{instance_url}/api/now/table/cmdb_ci/{ci_id}"
        update_data = {
            'assigned_to': user_display_name  # Use display name instead of sys_id
        }
        
        update_response = requests.patch(
            ci_url,
            auth=(username, password),
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            },
            json=update_data,
            timeout=30
        )
        
        if update_response.status_code == 200:
            logger.info(f"Successfully assigned CI {ci_id} to user {user_display_name} (username: {new_owner_username})")
            return jsonify({
                'success': True,
                'message': f'CI successfully assigned to {user_display_name}',
                'new_owner': {
                    'username': new_owner_username,
                    'display_name': user_display_name,
                    'sys_id': user_sys_id
                }
            })
        else:
            error_msg = f"Failed to update CI assignment: {update_response.status_code}"
            logger.error(error_msg)
            return jsonify({'error': error_msg}), 500
            
    except Exception as e:
        logger.error(f"Error in assign_ci_owner: {str(e)}", exc_info=True)
        return jsonify({'error': f'Assignment failed: {str(e)}'}), 500

if __name__ == '__main__':
    print("Starting CMDB Analyzer Backend with ML Model...")
    print("Backend will be available at: http://localhost:5000")
    print("Health check: http://localhost:5000/health")
    print("Test connection: POST /test-connection")
    print("Scan for stale ownership: POST /scan-stale-ownership")
    app.run(debug=True, host='0.0.0.0', port=5000)