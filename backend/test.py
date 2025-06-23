#!/usr/bin/env python3
"""
Test script to verify the model loads correctly
Save this as test_model.py in your backend directory
"""

import unittest
import pandas as pd
import sys
import os
import pickle
import logging
from datetime import datetime, timedelta

# Add the current directory to Python path to find the module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from KB_Model import EnhancedStalenessDetector, ScenarioKnowledgeBase
from app import analyze_cis_with_model

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestCMDBAnalyzer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures before running tests"""
        try:
            # Load or create model
            model_path = 'KB_Model(2).pkl'
            if os.path.exists(model_path):
                with open(model_path, 'rb') as f:
                    cls.model = pickle.load(f)
            else:
                cls.model = EnhancedStalenessDetector()
                with open(model_path, 'wb') as f:
                    pickle.dump(cls.model, f)
        except Exception as e:
            print(f"Error setting up test class: {str(e)}")
            raise

    def setUp(self):
        """Set up test fixtures before each test"""
        # Sample test data
        self.ci_data = [{
            'sys_id': 'test123',
            'name': 'Test Server',
            'sys_class_name': 'cmdb_ci_server',
            'sys_updated_on': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'assigned_to': {
                'user_name': 'test.user',
                'name': 'Test User'
            }
        }]
        
        self.audit_data = [{
            'documentkey': 'test123',
            'sys_created_on': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S'),
            'user': 'different.user'
        }]
        
        self.user_data = [{
            'user_name': 'test.user',
            'name': 'Test User',
            'department': 'IT',
            'title': 'System Admin',
            'active': 'true'
        }]

    def test_model_initialization(self):
        """Test if model is properly initialized"""
        self.assertIsNotNone(self.model)
        self.assertIsInstance(self.model, EnhancedStalenessDetector)

    def test_single_prediction(self):
        """Test prediction for a single CI"""
        ci_info = {
            'ci_data': self.ci_data[0],
            'audit_data': self.audit_data,
            'user_data': self.user_data
        }
        
        prediction = self.model.predict_single(ci_info)
        
        # Test prediction structure
        self.assertIsInstance(prediction, dict)
        self.assertIn('is_stale', prediction)
        self.assertIn('confidence', prediction)
        self.assertIn('reasons', prediction)
        self.assertIn('staleness_score', prediction)  # New in KB_Model(2)
        self.assertIn('evidence_strength', prediction)  # New in KB_Model(2)
        self.assertIn('scenario_matches', prediction)  # New in KB_Model(2)

    def test_analyze_cis_with_model(self):
        """Test the main analysis function"""
        results = analyze_cis_with_model(self.ci_data, self.audit_data, self.user_data)
        
        # Test results structure
        self.assertIsInstance(results, list)
        if results:  # If any stale CIs found
            stale_ci = results[0]
            self.assertIn('ci_id', stale_ci)
            self.assertIn('ci_name', stale_ci)
            self.assertIn('current_owner', stale_ci)
            self.assertIn('staleness_score', stale_ci)  # New in KB_Model(2)
            self.assertIn('evidence_strength', stale_ci)  # New in KB_Model(2)
            self.assertIn('scenario_matches', stale_ci)  # New in KB_Model(2)

    def test_scenario_matching(self):
        """Test scenario matching functionality"""
        ci_info = {
            'ci_data': self.ci_data[0],
            'audit_data': self.audit_data,
            'user_data': self.user_data
        }
        
        prediction = self.model.predict_single(ci_info)
        
        # Test scenario matches
        self.assertIn('scenario_matches', prediction)
        if prediction['scenario_matches']:
            scenario = prediction['scenario_matches'][0]
            self.assertIn('name', scenario)
            self.assertIn('confidence', scenario)
            self.assertIn('context', scenario)

    def test_evidence_strength(self):
        """Test evidence strength calculation"""
        ci_info = {
            'ci_data': self.ci_data[0],
            'audit_data': self.audit_data,
            'user_data': self.user_data
        }
        
        prediction = self.model.predict_single(ci_info)
        
        # Test evidence strength
        self.assertIn('evidence_strength', prediction)
        self.assertIn(prediction['evidence_strength'], ['Strong', 'Medium', 'Weak'])

    def test_staleness_score(self):
        """Test staleness score calculation"""
        ci_info = {
            'ci_data': self.ci_data[0],
            'audit_data': self.audit_data,
            'user_data': self.user_data
        }
        
        prediction = self.model.predict_single(ci_info)
        
        # Test staleness score
        self.assertIn('staleness_score', prediction)
        self.assertIsInstance(prediction['staleness_score'], float)
        self.assertTrue(0.0 <= prediction['staleness_score'] <= 1.0)

    def tearDown(self):
        """Clean up after each test"""
        pass

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests"""
        pass

if __name__ == '__main__':
    unittest.main()