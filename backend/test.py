#!/usr/bin/env python3
"""
Test script to verify the model loads correctly
Save this as test_model.py in your backend directory
"""

import pickle
import os
from datetime import datetime

def test_model_loading():
    """Test if the model loads and works correctly"""
    
    print("Testing Model Loading...")
    print("=" * 50)
    
    # Check if pickle file exists
    model_path = 'enhanced_staleness_model.pkl'
    
    if not os.path.exists(model_path):
        print(f"❌ ERROR: {model_path} not found!")
        print(f"Please run: python enhanced_staleness_model.py to generate the model.")
        return False
    
    print(f"✅ Found pickle file: {model_path}")
    print(f"File size: {os.path.getsize(model_path)} bytes")
    
    # Try to load the model
    try:
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        print("✅ Model loaded successfully!")
        
        # Check if it has the required methods
        required_methods = ['predict_single', 'get_stale_ci_list']
        
        for method in required_methods:
            if hasattr(model, method):
                print(f"✅ Method {method} found")
            else:
                print(f"❌ Method {method} missing")
                return False
        
        # Test a simple prediction
        sample_data = {
            'assigned_owner': 'test.user',
            'audit_records': [
                {
                    'user': 'other.user',
                    'sys_created_on': '2024-06-20 10:30:00',
                    'fieldname': 'assigned_to'
                }
            ],
            'user_info': {
                'active': True
            }
        }
        
        print("\n🧪 Testing sample prediction...")
        result = model.predict_single(sample_data)
        
        if 'is_stale' in result and 'confidence' in result:
            print("✅ Sample prediction successful!")
            print(f"   Result: {result['is_stale']} (confidence: {result['confidence']:.2f})")
            return True
        else:
            print("❌ Sample prediction failed - invalid result format")
            return False
            
    except Exception as e:
        print(f"❌ ERROR loading model: {str(e)}")
        return False

def test_flask_integration():
    """Test if Flask can import the model properly"""
    print("\n🌐 Testing Flask Integration...")
    print("=" * 50)
    
    try:
        # Try importing Flask dependencies
        import flask
        import pandas as pd
        import numpy as np
        print("✅ All Flask dependencies available")
        
        # Test the model loading like Flask does
        MODEL_PATH = 'enhanced_staleness_model.pkl'
        
        with open(MODEL_PATH, 'rb') as f:
            model = pickle.load(f)
        
        print("✅ Model loads in Flask context")
        
        # Test if model works with pandas DataFrames (like Flask backend)
        if hasattr(model, 'get_stale_ci_list'):
            print("✅ get_stale_ci_list method available")
        else:
            print("❌ get_stale_ci_list method missing")
            return False
            
        return True
        
    except ImportError as e:
        print(f"❌ Missing dependency: {str(e)}")
        print("Please run: pip install flask pandas numpy scikit-learn")
        return False
    except Exception as e:
        print(f"❌ Flask integration test failed: {str(e)}")
        return False

if __name__ == "__main__":
    print(f"Model Loading Test - {datetime.now()}")
    print("=" * 60)
    
    # Test basic model loading
    model_ok = test_model_loading()
    
    # Test Flask integration
    flask_ok = test_flask_integration()
    
    print("\n" + "=" * 60)
    print("SUMMARY:")
    print("=" * 60)
    
    if model_ok and flask_ok:
        print("🎉 ALL TESTS PASSED!")
        print("Your model should work with the Flask backend.")
        print("\nNext steps:")
        print("1. Start your Flask app: python app.py")
        print("2. Check the health endpoint: curl http://localhost:5000/health")
        print("3. Look for 'model_loaded': true in the response")
    else:
        print("❌ TESTS FAILED!")
        if not model_ok:
            print("- Model loading failed")
        if not flask_ok:
            print("- Flask integration failed")
        print("\nTroubleshooting:")
        print("1. Recreate the model: python enhanced_staleness_model.py")
        print("2. Install dependencies: pip install flask pandas numpy scikit-learn")
        print("3. Check file permissions")