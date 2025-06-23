import pandas as pd
import numpy as np
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import re
from collections import defaultdict, Counter
import json
import warnings
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, precision_recall_fscore_support
import matplotlib.pyplot as plt
import seaborn as sns
warnings.filterwarnings('ignore')

class ScenarioBasedStalenessDetector:
    """
    Enhanced staleness detector that learns from specific audit patterns and scenarios
    """

    def __init__(self):
        self.scenario_patterns = {}
        self.feature_extractors = self._define_feature_extractors()
        self.confidence_thresholds = {
            'critical': 0.95,
            'high': 0.85,
            'medium': 0.75,
            'low': 0.65
        }
        self.trained = False
        self.training_stats = {}

    def _define_feature_extractors(self):
        """Define comprehensive feature extraction methods"""
        return {
            'temporal_features': self._extract_temporal_features,
            'behavioral_features': self._extract_behavioral_features,
            'organizational_features': self._extract_organizational_features,
            'account_features': self._extract_account_features,
            'activity_features': self._extract_activity_features
        }

    def train_from_data(self, labels_df, audit_df, user_df, ci_df):
        """
        Train the model by learning patterns from labeled data
        """
        print("Training scenario-based staleness detector...")
        print(f"Training data: {len(labels_df)} CIs, {len(audit_df)} audit records")
        
        # Extract patterns from training data
        scenario_data = self._extract_scenario_patterns(labels_df, audit_df, user_df, ci_df)
        
        # Learn patterns for each scenario type
        self._learn_scenario_patterns(scenario_data)
        
        # Store training statistics
        self.training_stats = {
            'total_training_cis': len(labels_df),
            'stale_training_cis': len([d for d in scenario_data if d['staleness_label'] == 1]),
            'non_stale_training_cis': len([d for d in scenario_data if d['staleness_label'] == 0]),
            'scenarios_learned': len(self.scenario_patterns)
        }
        
        self.trained = True
        print(f"Training completed. Learned {len(self.scenario_patterns)} scenario patterns.")
        
        return self

    def _extract_scenario_patterns(self, labels_df, audit_df, user_df, ci_df):
        """
        Extract and categorize patterns from the training data
        """
        print("Extracting scenario patterns...")
        
        # Build lookup dictionaries
        audit_by_ci = audit_df.groupby('documentkey').apply(lambda x: x.to_dict('records')).to_dict()
        
        # Handle duplicate user names by keeping the most recent entry
        user_df_dedup = user_df.drop_duplicates(subset=['user_name'], keep='last')
        user_by_name = user_df_dedup.set_index('user_name').to_dict('index')
        
        # Handle duplicate CI IDs by keeping the most recent entry
        ci_df_dedup = ci_df.drop_duplicates(subset=['sys_id'], keep='last')
        ci_by_id = ci_df_dedup.set_index('sys_id').to_dict('index')

        scenario_data = []
        
        for idx, label in labels_df.iterrows():
            ci_id = label.get('ci_id')
            ci_info = ci_by_id.get(str(ci_id), {})
            audit_records = audit_by_ci.get(str(ci_id), [])
            user_info = user_by_name.get(str(label.get('assigned_owner', '')), {})
            
            # Extract all features
            features = self._extract_all_features({
                'ci_info': ci_info,
                'audit_records': audit_records,
                'user_info': user_info,
                'assigned_owner': label.get('assigned_owner', '')
            })
            
            scenario_data.append({
                'features': features,
                'staleness_reasons': label.get('staleness_reasons', ''),
                'staleness_label': label.get('staleness_label'),
                'staleness_score': label.get('staleness_score', 0),
                'confidence': label.get('confidence', 0),
                'ci_id': ci_id,
                'assigned_owner': label.get('assigned_owner', '')
            })
        
        print(f"Extracted {len(scenario_data)} scenario instances")
        return scenario_data

    def _learn_scenario_patterns(self, scenario_data):
        """
        Learn patterns from scenario data by grouping similar reason patterns
        """
        print("Learning scenario patterns...")
        
        # Group scenarios by similar patterns (only from stale instances)
        reason_groups = defaultdict(list)
        
        for data in scenario_data:
            if data['staleness_label'] == 1 and data['staleness_reasons']:  # Only learn from stale instances with reasons
                # Normalize the reason string
                normalized_reason = self._normalize_reason_string(data['staleness_reasons'])
                reason_groups[normalized_reason].append(data)
        
        # Create scenario patterns
        scenario_id = 1
        for reason_pattern, instances in reason_groups.items():
            if len(instances) >= 1:  # Consider patterns with at least 1 instance
                
                # Extract common features from instances
                common_features = self._extract_common_features(instances)
                
                # Calculate pattern statistics
                avg_confidence = np.mean([inst['confidence'] for inst in instances])
                avg_score = np.mean([inst['staleness_score'] for inst in instances])
                
                self.scenario_patterns[f"scenario_{scenario_id}"] = {
                    'reason_pattern': reason_pattern,
                    'original_reasons': [inst['staleness_reasons'] for inst in instances],
                    'common_features': common_features,
                    'feature_thresholds': self._calculate_feature_thresholds(instances),
                    'instance_count': len(instances),
                    'avg_confidence': avg_confidence,
                    'avg_score': avg_score,
                    'feature_weights': self._calculate_feature_weights(instances)
                }
                scenario_id += 1
        
        print(f"Learned {len(self.scenario_patterns)} unique scenario patterns")

    def _normalize_reason_string(self, reason_string):
        """
        Normalize reason strings to group similar patterns
        """
        if not reason_string or pd.isna(reason_string):
            return ""
        
        # Split by semicolons and normalize each part
        parts = [part.strip() for part in str(reason_string).split(';')]
        
        # Normalize temporal references
        normalized_parts = []
        for part in parts:
            # Replace specific day counts with generic patterns
            part = re.sub(r'\d+ days', 'X days', part)
            part = re.sub(r'>\d+ months?', '>X months', part)
            part = re.sub(r'>\d+ years?', '>X years', part)
            normalized_parts.append(part)
        
        return '; '.join(sorted(normalized_parts))

    def _extract_common_features(self, instances):
        """
        Extract features that are common across instances of a scenario
        """
        if not instances:
            return {}
        
        # Get all feature keys
        all_features = set()
        for inst in instances:
            all_features.update(inst['features'].keys())
        
        common_features = {}
        for feature in all_features:
            values = [inst['features'].get(feature) for inst in instances if feature in inst['features']]
            
            if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in values if v is not None):
                # Numerical feature
                values = [v for v in values if v is not None]
                if values:
                    common_features[feature] = {
                        'type': 'numerical',
                        'mean': np.mean(values),
                        'std': np.std(values) if len(values) > 1 else 0,
                        'min': min(values),
                        'max': max(values)
                    }
            elif all(isinstance(v, bool) for v in values if v is not None):
                # Boolean feature
                values = [v for v in values if v is not None]
                if values:
                    common_features[feature] = {
                        'type': 'boolean',
                        'true_ratio': sum(values) / len(values)
                    }
            else:
                # Categorical feature
                values = [str(v) for v in values if v is not None]
                if values:
                    value_counts = Counter(values)
                    common_features[feature] = {
                        'type': 'categorical',
                        'most_common': value_counts.most_common(3),
                        'unique_count': len(value_counts)
                    }
        
        return common_features

    def _calculate_feature_thresholds(self, instances):
        """
        Calculate thresholds for feature matching
        """
        thresholds = {}
        
        for inst in instances:
            for feature, value in inst['features'].items():
                if isinstance(value, (int, float)) and not isinstance(value, bool) and feature not in thresholds:
                    all_values = [i['features'].get(feature) for i in instances if feature in i['features']]
                    all_values = [v for v in all_values if isinstance(v, (int, float)) and not isinstance(v, bool)]
                    
                    if all_values:
                        thresholds[feature] = {
                            'min_threshold': min(all_values) * 0.8,  # 20% tolerance
                            'max_threshold': max(all_values) * 1.2,
                            'mean': np.mean(all_values)
                        }
        
        return thresholds

    def _calculate_feature_weights(self, instances):
        """
        Calculate importance weights for features based on their discriminative power
        """
        weights = {}
        
        # Features that appear in staleness reasons get higher weights
        reason_features = {
            'owner_activity_count': 2.0,
            'days_since_owner_activity': 1.8,
            'owner_active': 1.5,
            'top_other_user_ratio': 1.3,
            'generic_account_indicator': 1.4,
            'vendor_account_indicator': 1.4,
            'non_owner_ownership_changes': 1.2,
            'owner_has_no_activity': 2.0,
            'account_terminated': 1.5,
            'account_not_found': 1.4
        }
        
        # Set default weights
        for inst in instances:
            for feature in inst['features'].keys():
                weights[feature] = reason_features.get(feature, 1.0)
        
        return weights

    def _extract_all_features(self, ci_data):
        """
        Extract all features using the defined extractors
        """
        all_features = {}
        
        for extractor_name, extractor_func in self.feature_extractors.items():
            try:
                features = extractor_func(ci_data)
                all_features.update(features)
            except Exception as e:
                print(f"Warning: Error in {extractor_name}: {e}")
                continue
        
        return all_features

    def _extract_temporal_features(self, ci_data):
        """Extract time-based features"""
        features = {}
        
        audit_records = ci_data.get('audit_records', [])
        assigned_owner = ci_data.get('assigned_owner', '')
        
        # Days since any activity
        if audit_records:
            try:
                dates = []
                for r in audit_records:
                    date_str = r.get('sys_created_on', '')
                    if date_str:
                        parsed_date = self._parse_date(date_str)
                        dates.append(parsed_date)
                
                if dates:
                    latest_activity = max(dates)
                    features['days_since_last_activity'] = (datetime.now() - latest_activity).days
                else:
                    features['days_since_last_activity'] = 999
            except:
                features['days_since_last_activity'] = 999
        else:
            features['days_since_last_activity'] = 999
        
        # Days since owner activity
        owner_activities = [r for r in audit_records if r.get('user') == assigned_owner]
        if owner_activities:
            try:
                dates = []
                for r in owner_activities:
                    date_str = r.get('sys_created_on', '')
                    if date_str:
                        parsed_date = self._parse_date(date_str)
                        dates.append(parsed_date)
                
                if dates:
                    last_owner_activity = max(dates)
                    features['days_since_owner_activity'] = (datetime.now() - last_owner_activity).days
                else:
                    features['days_since_owner_activity'] = 999
            except:
                features['days_since_owner_activity'] = 999
        else:
            features['days_since_owner_activity'] = 999
        
        # Temporal categories
        features['no_activity_3_months'] = features['days_since_last_activity'] > 90
        features['no_activity_6_months'] = features['days_since_last_activity'] > 180
        features['no_activity_1_year'] = features['days_since_last_activity'] > 365
        features['no_owner_activity_30_days'] = features['days_since_owner_activity'] > 30
        
        return features

    def _extract_behavioral_features(self, ci_data):
        """Extract behavior-based features"""
        features = {}
        
        audit_records = ci_data.get('audit_records', [])
        assigned_owner = ci_data.get('assigned_owner', '')
        
        total_activities = len(audit_records)
        owner_activities = [r for r in audit_records if r.get('user') == assigned_owner]
        
        features['total_activity_count'] = total_activities
        features['owner_activity_count'] = len(owner_activities)
        
        if total_activities > 0:
            features['owner_activity_ratio'] = len(owner_activities) / total_activities
        else:
            features['owner_activity_ratio'] = 0
        
        # Other user analysis
        other_users = {}
        system_activities = 0
        
        for record in audit_records:
            user = record.get('user', '')
            if user == 'system':
                system_activities += 1
            elif user != assigned_owner and user:
                other_users[user] = other_users.get(user, 0) + 1
        
        features['other_users_count'] = len(other_users)
        features['system_activity_count'] = system_activities
        features['system_activity_ratio'] = system_activities / total_activities if total_activities > 0 else 0
        
        # Dominant user features
        if other_users:
            top_user_count = max(other_users.values())
            features['top_other_user_count'] = top_user_count
            features['top_other_user_ratio'] = top_user_count / total_activities if total_activities > 0 else 0
        else:
            features['top_other_user_count'] = 0
            features['top_other_user_ratio'] = 0
        
        # Behavioral patterns
        features['multiple_users_active'] = len(other_users) > 1
        features['owner_minimal_activity'] = features['owner_activity_ratio'] < 0.3 and features['owner_activity_count'] <= 1
        features['predominantly_system_activities'] = features['system_activity_ratio'] > 0.6
        features['unclear_ownership'] = features['multiple_users_active'] and features['owner_activity_ratio'] < 0.5
        
        return features

    def _extract_organizational_features(self, ci_data):
        """Extract organization-based features"""
        features = {}
        
        assigned_owner = str(ci_data.get('assigned_owner', '')).lower()
        user_info = ci_data.get('user_info', {})
        
        # Account type indicators
        features['generic_account_indicator'] = any(term in assigned_owner for term in ['.generic', 'admin.generic', 'team.generic'])
        features['vendor_account_indicator'] = any(term in assigned_owner for term in ['vendor.', 'external.', '.contractor'])
        features['system_account_indicator'] = 'system' in assigned_owner
        
        # User directory status
        features['owner_in_user_directory'] = bool(user_info)
        features['owner_active'] = user_info.get('active', False) if user_info else False
        
        # Department and role changes (simplified)
        audit_records = ci_data.get('audit_records', [])
        org_change_fields = ['department', 'title', 'role', 'manager', 'location']
        org_changes = sum(1 for r in audit_records if str(r.get('fieldname', '')).lower() in org_change_fields)
        features['organizational_changes'] = org_changes
        features['has_organizational_changes'] = org_changes > 0
        
        return features

    def _extract_account_features(self, ci_data):
        """Extract account-specific features"""
        features = {}
        
        assigned_owner = ci_data.get('assigned_owner', '')
        user_info = ci_data.get('user_info', {})
        
        # Account status
        features['account_active'] = user_info.get('active', False) if user_info else False
        features['account_terminated'] = not features['account_active'] and bool(user_info)
        features['account_not_found'] = not bool(user_info)
        
        # Account creation patterns
        if user_info.get('sys_created_on'):
            try:
                created_date = self._parse_date(user_info['sys_created_on'])
                features['account_age_days'] = (datetime.now() - created_date).days
                features['new_account'] = features['account_age_days'] < 30
            except:
                features['account_age_days'] = 999
                features['new_account'] = False
        else:
            features['account_age_days'] = 0
            features['new_account'] = False
        
        return features

    def _extract_activity_features(self, ci_data):
        """Extract activity pattern features"""
        features = {}
        
        audit_records = ci_data.get('audit_records', [])
        assigned_owner = ci_data.get('assigned_owner', '')
        
        # Ownership field changes
        ownership_fields = ['assigned_to', 'managed_by', 'support_group', 'owner']
        ownership_changes_by_owner = 0
        ownership_changes_by_others = 0
        
        for record in audit_records:
            field_name = str(record.get('fieldname', '')).lower()
            if field_name in ownership_fields:
                if record.get('user') == assigned_owner:
                    ownership_changes_by_owner += 1
                else:
                    ownership_changes_by_others += 1
        
        features['ownership_changes_by_owner'] = ownership_changes_by_owner
        features['ownership_changes_by_others'] = ownership_changes_by_others
        features['non_owner_ownership_changes'] = ownership_changes_by_others
        
        # Activity patterns
        features['owner_has_no_activity'] = len([r for r in audit_records if r.get('user') == assigned_owner]) == 0
        
        # Recent activity analysis (last 30 days)
        recent_cutoff = datetime.now() - timedelta(days=30)
        recent_activities = 0
        recent_owner_activities = 0
        
        for record in audit_records:
            try:
                record_date = self._parse_date(record.get('sys_created_on', ''))
                if record_date > recent_cutoff:
                    recent_activities += 1
                    if record.get('user') == assigned_owner:
                        recent_owner_activities += 1
            except:
                pass
        
        features['recent_total_activities'] = recent_activities
        features['recent_owner_activities'] = recent_owner_activities
        features['no_recent_owner_activity'] = recent_owner_activities == 0
        
        return features

    def predict_single(self, ci_data):
        """
        Predict staleness for a single CI using learned scenario patterns
        """
        if not self.trained:
            raise ValueError("Model must be trained before making predictions")
        
        # Extract features
        features = self._extract_all_features(ci_data)
        
        # Match against learned scenarios
        scenario_matches = []
        max_confidence = 0
        
        for scenario_id, scenario_pattern in self.scenario_patterns.items():
            match_score = self._calculate_scenario_match_score(features, scenario_pattern)
            
            if match_score > 0.3:  # Minimum threshold for considering a match
                confidence = min(0.98, scenario_pattern['avg_confidence'] * match_score)
                scenario_matches.append({
                    'scenario_id': scenario_id,
                    'match_score': match_score,
                    'confidence': confidence,
                    'reason_pattern': scenario_pattern['reason_pattern'],
                    'original_reasons': scenario_pattern['original_reasons'][0]  # Take first example
                })
                max_confidence = max(max_confidence, confidence)
        
        # Sort by match score
        scenario_matches.sort(key=lambda x: x['match_score'], reverse=True)
        
        # Determine staleness
        is_stale = max_confidence > 0.7
        
        # Generate explanation
        if scenario_matches:
            best_match = scenario_matches[0]
            explanation = self._generate_explanation(features, best_match)
        else:
            explanation = "No matching scenario patterns found"
        
        # Recommend new owner
        new_owner_recommendation = self._recommend_new_owner_from_data(ci_data)
        
        return {
            'is_stale': is_stale,
            'confidence': max_confidence,
            'matched_scenarios': scenario_matches[:3],  # Top 3 matches
            'explanation': explanation,
            'new_owner_recommendation': new_owner_recommendation,
            'features': features
        }

    def predict_batch(self, ci_data_list):
        """
        Predict staleness for multiple CIs
        """
        results = []
        for ci_data in ci_data_list:
            result = self.predict_single(ci_data)
            results.append(result)
        return results

    def _calculate_scenario_match_score(self, features, scenario_pattern):
        """
        Calculate how well features match a scenario pattern
        """
        common_features = scenario_pattern['common_features']
        feature_weights = scenario_pattern['feature_weights']
        thresholds = scenario_pattern['feature_thresholds']
        
        total_weight = 0
        weighted_score = 0
        
        for feature_name, feature_data in common_features.items():
            if feature_name in features:
                weight = feature_weights.get(feature_name, 1.0)
                total_weight += weight
                
                feature_value = features[feature_name]
                
                if feature_data['type'] == 'numerical':
                    # Numerical feature matching
                    if feature_name in thresholds:
                        threshold_data = thresholds[feature_name]
                        if threshold_data['min_threshold'] <= feature_value <= threshold_data['max_threshold']:
                            score = 1.0
                        else:
                            # Calculate distance-based score
                            distance = min(
                                abs(feature_value - threshold_data['min_threshold']),
                                abs(feature_value - threshold_data['max_threshold'])
                            )
                            max_distance = max(threshold_data['max_threshold'] - threshold_data['min_threshold'], 1)
                            score = max(0, 1.0 - (distance / max_distance))
                    else:
                        score = 0.5  # Default score for numerical without thresholds
                
                elif feature_data['type'] == 'boolean':
                    # Boolean feature matching
                    if isinstance(feature_value, bool):
                        expected_ratio = feature_data['true_ratio']
                        if (feature_value and expected_ratio > 0.5) or (not feature_value and expected_ratio <= 0.5):
                            score = 1.0
                        else:
                            score = 0.0
                    else:
                        score = 0.5
                
                else:  # categorical
                    # Categorical feature matching
                    most_common_values = [item[0] for item in feature_data['most_common']]
                    if str(feature_value) in most_common_values:
                        score = 1.0
                    else:
                        score = 0.0
                
                weighted_score += score * weight
        
        return weighted_score / total_weight if total_weight > 0 else 0

    def _generate_explanation(self, features, best_match):
        """
        Generate human-readable explanation for the staleness prediction
        """
        explanations = []
        
        # Add specific feature-based explanations
        if features.get('owner_has_no_activity', False):
            explanations.append("Assigned owner has no recorded activity on CI")
        
        if features.get('days_since_owner_activity', 0) > 90:
            days = features['days_since_owner_activity']
            if days > 365:
                explanations.append(f"No activity in {days} days (>1 year)")
            elif days > 180:
                explanations.append(f"No activity in {days} days (>6 months)")
            else:
                explanations.append(f"No activity in {days} days (>3 months)")
        
        if features.get('account_terminated', False):
            explanations.append("Owner account is inactive/terminated")
        
        if features.get('account_not_found', False):
            explanations.append("Owner not found in user directory")
        
        if features.get('generic_account_indicator', False):
            explanations.append("Assigned to generic team account")
        
        if features.get('vendor_account_indicator', False):
            explanations.append("Assigned to external/contractor account")
        
        if features.get('multiple_users_active', False):
            explanations.append("Multiple users active - unclear ownership")
        
        if features.get('owner_minimal_activity', False):
            explanations.append("Owner minimal activity compared to others")
        
        if features.get('predominantly_system_activities', False):
            explanations.append("Predominantly system activities - organizational change")
        
        return "; ".join(explanations) if explanations else best_match['original_reasons']

    def _recommend_new_owner_from_data(self, ci_data):
        """Recommend new owner based on activity patterns"""
        try:
            audit_records = ci_data.get('audit_records', [])
            assigned_owner = ci_data.get('assigned_owner', '')
            
            if not audit_records:
                return None

            # Analyze user activities (excluding current owner and system)
            user_activities = {}
            for record in audit_records:
                user = record.get('user', '')
                if user and user != assigned_owner and user != 'system':
                    if user not in user_activities:
                        user_activities[user] = {
                            'count': 0,
                            'last_activity': None,
                            'ownership_changes': 0
                        }
                    
                    user_activities[user]['count'] += 1
                    
                    # Track ownership field changes
                    if str(record.get('fieldname', '')).lower() in ['assigned_to', 'managed_by', 'support_group']:
                        user_activities[user]['ownership_changes'] += 1
                    
                    try:
                        activity_date = self._parse_date(record.get('sys_created_on', ''))
                        if user_activities[user]['last_activity'] is None or activity_date > user_activities[user]['last_activity']:
                            user_activities[user]['last_activity'] = activity_date
                    except:
                        pass

            if not user_activities:
                return None

            # Score users
            best_user = None
            best_score = 0

            for user, data in user_activities.items():
                score = 0
                
                # Activity volume (40%)
                activity_ratio = data['count'] / len(audit_records)
                score += activity_ratio * 40
                
                # Recency (30%)
                if data['last_activity']:
                    days_since = (datetime.now() - data['last_activity']).days
                    recency_score = max(0, (30 - days_since) / 30)  # Score higher for recent activity
                    score += recency_score * 30
                
                # Ownership changes (30%)
                if data['ownership_changes'] > 0:
                    score += 30

                if score > best_score:
                    best_score = score
                    best_user = user

            if best_user:
                return {
                    'user': best_user,
                    'score': best_score,
                    'activity_count': user_activities[best_user]['count'],
                    'ownership_changes': user_activities[best_user]['ownership_changes'],
                    'last_activity_days_ago': (datetime.now() - user_activities[best_user]['last_activity']).days if user_activities[best_user]['last_activity'] else 999
                }

            return None

        except Exception as e:
            return None

    def _parse_date(self, date_str):
        """Parse ServiceNow date format"""
        try:
            if pd.isna(date_str) or not date_str:
                return datetime(1900, 1, 1)
                
            date_str = str(date_str).strip()
            
            formats = [
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d',
                '%m/%d/%Y %H:%M:%S',
                '%m/%d/%Y'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            
            return datetime(1900, 1, 1)
        except:
            return datetime(1900, 1, 1)

    def save_model(self, filepath):
        """Save the trained model"""
        with open(filepath, 'wb') as f:
            pickle.dump(self, f)
        print(f"Model saved to {filepath}")

    @classmethod
    def load_model(cls, filepath):
        """Load a trained model"""
        with open(filepath, 'rb') as f:
            model = pickle.load(f)
        print(f"Model loaded from {filepath}")
        return model

    def get_training_summary(self):
        """Get summary of training data"""
        if not self.trained:
            return "Model not trained yet"
        
        summary = {
            'total_scenarios': len(self.scenario_patterns),
            'training_stats': self.training_stats,
            'scenario_details': []
        }
        
        for scenario_id, pattern in self.scenario_patterns.items():
            summary['scenario_details'].append({
                'scenario_id': scenario_id,
                'reason_pattern': pattern['reason_pattern'],
                'instance_count': pattern['instance_count'],
                'avg_confidence': pattern['avg_confidence'],
                'avg_score': pattern['avg_score']
            })
        
        return summary

    def get_stale_ci_list(self, labels_df, audit_df, user_df, ci_df):
        """
        Process a batch of CIs and return a list of stale CIs with their details
        """
        # Build lookup dictionaries
        audit_by_ci = audit_df.groupby('documentkey').apply(lambda x: x.to_dict('records')).to_dict()
        
        # Handle duplicate user names by keeping the most recent entry
        user_df_dedup = user_df.drop_duplicates(subset=['user_name'], keep='last')
        user_by_name = user_df_dedup.set_index('user_name').to_dict('index')
        
        # Handle duplicate CI IDs by keeping the most recent entry
        ci_df_dedup = ci_df.drop_duplicates(subset=['sys_id'], keep='last')
        ci_by_id = ci_df_dedup.set_index('sys_id').to_dict('index')

        stale_ci_list = []
        
        for idx, label in labels_df.iterrows():
            ci_id = label.get('ci_id')
            assigned_owner = label.get('assigned_owner', '')
            
            ci_info = ci_by_id.get(str(ci_id), {})
            audit_records = audit_by_ci.get(str(ci_id), [])
            user_info = user_by_name.get(str(assigned_owner), {})
            
            # Prepare data for prediction
            ci_data = {
                'ci_info': ci_info,
                'audit_records': audit_records,
                'user_info': user_info,
                'assigned_owner': assigned_owner
            }
            
            # Get prediction
            prediction = self.predict_single(ci_data)
            
            # If predicted as stale, add to the list
            if prediction.get('is_stale', False):
                # Calculate risk level based on confidence
                confidence = prediction.get('confidence', 0)
                if confidence >= self.confidence_thresholds['critical']:
                    risk_level = 'Critical'
                elif confidence >= self.confidence_thresholds['high']:
                    risk_level = 'High'
                elif confidence >= self.confidence_thresholds['medium']:
                    risk_level = 'Medium'
                else:
                    risk_level = 'Low'
                
                stale_ci_list.append({
                    'ci_id': ci_id,
                    'name': ci_info.get('name', ''),
                    'sys_class_name': ci_info.get('sys_class_name', ''),
                    'assigned_to': assigned_owner,
                    'confidence': confidence,
                    'risk_level': risk_level,
                    'explanation': prediction.get('explanation', ''),
                    'recommended_action': prediction.get('recommended_action', ''),
                    'recommended_owner': prediction.get('recommended_owner', '')
                })
        
        return stale_ci_list


def split_data(labels_df, test_size=0.15, val_size=0.15, random_state=42):
    """
    Split data into train (70%), test (15%), and validation (15%) sets
    Maintains stratification based on staleness_label
    """
    print("Splitting data into train/test/validation sets...")
    
    # First split: separate train from test+val
    train_size = 1.0 - test_size - val_size
    temp_size = test_size + val_size
    
    X = labels_df.drop('staleness_label', axis=1) if 'staleness_label' in labels_df.columns else labels_df
    y = labels_df['staleness_label'] if 'staleness_label' in labels_df.columns else pd.Series([0] * len(labels_df))
    
    X_train, X_temp, y_train, y_temp = train_test_split(
        X, y, 
        test_size=temp_size, 
        random_state=random_state, 
        stratify=y
    )
    
    # Second split: separate test from validation
    test_ratio = test_size / temp_size
    
    X_test, X_val, y_test, y_val = train_test_split(
        X_temp, y_temp,
        test_size=(1 - test_ratio),
        random_state=random_state,
        stratify=y_temp
    )
    
    # Recreate DataFrames with labels
    train_df = X_train.copy()
    train_df['staleness_label'] = y_train
    
    test_df = X_test.copy()
    test_df['staleness_label'] = y_test
    
    val_df = X_val.copy()
    val_df['staleness_label'] = y_val
    
    print(f"Data split completed:")
    print(f"  Training set: {len(train_df)} samples ({len(train_df)/len(labels_df)*100:.1f}%)")
    print(f"    - Stale: {sum(y_train)} ({sum(y_train)/len(y_train)*100:.1f}%)")
    print(f"    - Non-stale: {len(y_train) - sum(y_train)} ({(len(y_train) - sum(y_train))/len(y_train)*100:.1f}%)")
    
    print(f"  Test set: {len(test_df)} samples ({len(test_df)/len(labels_df)*100:.1f}%)")
    print(f"    - Stale: {sum(y_test)} ({sum(y_test)/len(y_test)*100:.1f}%)")
    print(f"    - Non-stale: {len(y_test) - sum(y_test)} ({(len(y_test) - sum(y_test))/len(y_test)*100:.1f}%)")
    
    print(f"  Validation set: {len(val_df)} samples ({len(val_df)/len(labels_df)*100:.1f}%)")
    print(f"    - Stale: {sum(y_val)} ({sum(y_val)/len(y_val)*100:.1f}%)")
    print(f"    - Non-stale: {len(y_val) - sum(y_val)} ({(len(y_val) - sum(y_val))/len(y_val)*100:.1f}%)")
    
    return train_df, test_df, val_df


def prepare_test_data(test_df, audit_df, user_df, ci_df):
    """
    Prepare test data for prediction
    """
    print("Preparing test data...")
    
    # Build lookup dictionaries with duplicate handling
    audit_by_ci = audit_df.groupby('documentkey').apply(lambda x: x.to_dict('records')).to_dict()
    
    # Handle duplicate user names by keeping the most recent entry
    user_df_dedup = user_df.drop_duplicates(subset=['user_name'], keep='last')
    user_by_name = user_df_dedup.set_index('user_name').to_dict('index')
    
    # Handle duplicate CI IDs by keeping the most recent entry  
    ci_df_dedup = ci_df.drop_duplicates(subset=['sys_id'], keep='last')
    ci_by_id = ci_df_dedup.set_index('sys_id').to_dict('index')

    test_data = []
    
    for idx, label in test_df.iterrows():
        ci_id = label.get('ci_id')
        assigned_owner = label.get('assigned_owner', '')
        
        ci_info = ci_by_id.get(str(ci_id), {})
        audit_records = audit_by_ci.get(str(ci_id), [])
        user_info = user_by_name.get(str(assigned_owner), {})
        
        ci_data = {
            'ci_info': ci_info,
            'audit_records': audit_records,
            'user_info': user_info,
            'assigned_owner': assigned_owner
        }
        
        test_data.append({
            'ci_data': ci_data,
            'true_label': label.get('staleness_label'),
            'ci_id': ci_id,
            'true_reasons': label.get('staleness_reasons', ''),
            'true_confidence': label.get('confidence', 0)
        })
    
    return test_data


def evaluate_model(detector, test_data):
    """
    Evaluate the model on test data
    """
    print("Evaluating model on test data...")
    
    y_true = []
    y_pred = []
    y_pred_proba = []
    detailed_results = []
    
    for item in test_data:
        true_label = item['true_label']
        result = detector.predict_single(item['ci_data'])
        
        predicted_label = 1 if result['is_stale'] else 0
        confidence = result['confidence']
        
        y_true.append(true_label)
        y_pred.append(predicted_label)
        y_pred_proba.append(confidence)
        
        detailed_results.append({
            'ci_id': item['ci_id'],
            'true_label': true_label,
            'predicted_label': predicted_label,
            'confidence': confidence,
            'true_reasons': item['true_reasons'],
            'predicted_explanation': result['explanation'],
            'matched_scenarios': len(result['matched_scenarios'])
        })
    
    # Calculate metrics
    accuracy = accuracy_score(y_true, y_pred)
    precision, recall, f1, support = precision_recall_fscore_support(y_true, y_pred, average='weighted')
    
    print(f"\n📊 Model Evaluation Results:")
    print(f"Accuracy: {accuracy:.3f}")
    print(f"Precision: {precision:.3f}")
    print(f"Recall: {recall:.3f}")
    print(f"F1-Score: {f1:.3f}")
    
    # Classification report
    print(f"\n📋 Detailed Classification Report:")
    print(classification_report(y_true, y_pred, target_names=['Non-Stale', 'Stale']))
    
    # Confusion matrix
    cm = confusion_matrix(y_true, y_pred)
    print(f"\n🔢 Confusion Matrix:")
    print(f"                 Predicted")
    print(f"Actual    Non-Stale  Stale")
    print(f"Non-Stale    {cm[0,0]:4d}    {cm[0,1]:4d}")
    print(f"Stale        {cm[1,0]:4d}    {cm[1,1]:4d}")
    
    # Plot confusion matrix
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', 
                xticklabels=['Non-Stale', 'Stale'], 
                yticklabels=['Non-Stale', 'Stale'])
    plt.title('Confusion Matrix')
    plt.ylabel('Actual Label')
    plt.xlabel('Predicted Label')
    plt.show()
    
    # Show some example predictions
    print(f"\n🔍 Sample Predictions:")
    for i, result in enumerate(detailed_results[:5]):
        print(f"\n{i+1}. CI: {result['ci_id']}")
        print(f"   True: {result['true_label']}, Predicted: {result['predicted_label']}, Confidence: {result['confidence']:.3f}")
        print(f"   Explanation: {result['predicted_explanation'][:100]}...")
    
    return {
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'confusion_matrix': cm,
        'detailed_results': detailed_results
    }


# Main execution code for Google Colab with train/test/validation split
def main():
    """
    Main function to run in Google Colab with proper data splitting
    """
    print("Enhanced Scenario-Based Staleness Detector with Train/Test/Validation Split")
    print("=" * 80)
    
    # Load the data
    try:
        print("Loading data...")
        labels_df = pd.read_csv('cmdb_staleness_labels.csv')
        audit_df = pd.read_csv('updated_sys_audit.csv') 
        ci_df = pd.read_csv('synthetic_cmdb_ci.csv')
        user_df = pd.read_csv('synthetic_sys_user.csv')
        
        print(f"✓ Data loaded successfully:")
        print(f"  - Labels: {len(labels_df)} records")
        print(f"  - Audit: {len(audit_df)} records") 
        print(f"  - CI: {len(ci_df)} records")
        print(f"  - User: {len(user_df)} records")
        
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print("Please ensure all CSV files are uploaded to the Colab environment:")
        print("- cmdb_staleness_labels.csv")
        print("- updated_sys_audit.csv")
        print("- synthetic_cmdb_ci.csv")
        print("- synthetic_sys_user.csv")
        return
    
    # Split the data
    print("\n" + "=" * 80)
    train_df, test_df, val_df = split_data(labels_df)
    
    # Create and train the enhanced model
    print("\n" + "=" * 80)
    detector = ScenarioBasedStalenessDetector()
    detector.train_from_data(train_df, audit_df, user_df, ci_df)
    
    # Save the trained model
    model_filename = 'enhanced_staleness_model.pkl'
    detector.save_model(model_filename)
    
    # Get training summary
    summary = detector.get_training_summary()
    print(f"\n📊 Training Summary:")
    print(f"Total scenarios learned: {summary['total_scenarios']}")
    print(f"Training CIs: {summary['training_stats']['total_training_cis']}")
    print(f"  - Stale: {summary['training_stats']['stale_training_cis']}")
    print(f"  - Non-stale: {summary['training_stats']['non_stale_training_cis']}")
    
    # Show top 10 most common scenarios
    sorted_scenarios = sorted(summary['scenario_details'], key=lambda x: x['instance_count'], reverse=True)
    print(f"\n🔝 Top 10 Most Common Scenarios:")
    for i, scenario in enumerate(sorted_scenarios[:10]):
        print(f"{i+1:2d}. {scenario['scenario_id']}: {scenario['instance_count']} instances")
        print(f"     Avg Confidence: {scenario['avg_confidence']:.3f}")
        print(f"     Pattern: {scenario['reason_pattern'][:80]}...")
        print()
    
    # Evaluate on test data
    print("\n" + "=" * 80)
    test_data = prepare_test_data(test_df, audit_df, user_df, ci_df)
    test_results = evaluate_model(detector, test_data)
    
    # Evaluate on validation data
    print("\n" + "=" * 80)
    print("Evaluating on validation data...")
    val_data = prepare_test_data(val_df, audit_df, user_df, ci_df)
    val_results = evaluate_model(detector, val_data)
    
    print(f"\n✅ Model training and evaluation completed successfully!")
    print(f"Model saved as: {model_filename}")
    
    print(f"\n📈 Final Results Summary:")
    print(f"Test Set Performance:")
    print(f"  - Accuracy: {test_results['accuracy']:.3f}")
    print(f"  - F1-Score: {test_results['f1']:.3f}")
    print(f"Validation Set Performance:")
    print(f"  - Accuracy: {val_results['accuracy']:.3f}")
    print(f"  - F1-Score: {val_results['f1']:.3f}")
    
    return detector, test_results, val_results

# Run the main function if this script is executed directly
if __name__ == "__main__":
    detector, test_results, val_results = main()