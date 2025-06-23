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

class ScenarioKnowledgeBase:
    """
    Knowledge Base containing 25 scenarios from CI Ownership Conflict Scenarios document
    """
    
    def __init__(self):
        self.scenarios = self._initialize_scenarios()
        
    def _initialize_scenarios(self):
        """Initialize all 25 scenarios with their patterns and contexts"""
        scenarios = {
            "scenario_1": {
                "name": "Developer Promotion to Team Lead",
                "pattern": {
                    "role_change": True,
                    "increased_activity": True,
                    "configuration_changes": True,
                    "keywords": ["promotion", "team lead", "role change"]
                },
                "context": "Junior developer promoted to Team Lead, inherited server responsibility but CMDB shows previous lead as owner",
                "indicators": [
                    "User has new role/title in recent period",
                    "Increased configuration changes to servers",
                    "Installation of new development tools",
                    "Previous owner no longer active"
                ]
            },
            "scenario_2": {
                "name": "Employee Onboarding - New Hire Assignment",
                "pattern": {
                    "new_user": True,
                    "manager_assignment": True,
                    "daily_activities": True,
                    "keywords": ["onboarding", "new hire", "assignment"]
                },
                "context": "New DBA joined and was assigned database server during onboarding, but HR assigned it to manager instead",
                "indicators": [
                    "User account created recently",
                    "Daily database maintenance tasks",
                    "Regular backups and optimizations",
                    "Manager has no actual activity"
                ]
            },
            "scenario_3": {
                "name": "Department Reorganization",
                "pattern": {
                    "org_change": True,
                    "group_change": True,
                    "continued_activity": True,
                    "keywords": ["reorganization", "department", "team split"]
                },
                "context": "Department split into specialized teams, but servers still show old group ownership",
                "indicators": [
                    "User group membership changed",
                    "New specialized software installations",
                    "Continued management by same person",
                    "Old group no longer exists"
                ]
            },
            "scenario_4": {
                "name": "Contractor to Full-Time Conversion",
                "pattern": {
                    "account_type_change": True,
                    "access_level_change": True,
                    "contractor_pattern": True,
                    "keywords": ["contractor", "full-time", "conversion"]
                },
                "context": "Contractor hired full-time with enhanced access, but CMDB reflects contractor status",
                "indicators": [
                    "Account name changed (removed .contractor)",
                    "Enhanced access permissions",
                    "Production deployment access",
                    "On-call rotation participation"
                ]
            },
            "scenario_5": {
                "name": "Manager Departure - Ownership Vacuum",
                "pattern": {
                    "terminated_owner": True,
                    "multiple_users_active": True,
                    "no_clear_owner": True,
                    "keywords": ["departure", "terminated", "vacuum"]
                },
                "context": "IT Manager left abruptly, team members managing infrastructure without official assignment",
                "indicators": [
                    "Current owner account inactive/terminated",
                    "Multiple team members making changes",
                    "Rotating responsibility patterns",
                    "Escalations to interim manager"
                ]
            },
            "scenario_6": {
                "name": "Project Team Dissolution",
                "pattern": {
                    "team_disbanded": True,
                    "reassignment": True,
                    "repurposed_resources": True,
                    "keywords": ["dissolution", "disbanded", "project end"]
                },
                "context": "Special Projects team completed and dissolved, members reassigned but servers still assigned to non-existent team",
                "indicators": [
                    "Team/group no longer exists",
                    "Server repurposed for new function",
                    "Configuration changes for new purpose",
                    "User now in different department"
                ]
            },
            "scenario_7": {
                "name": "Cross-Training Assignment",
                "pattern": {
                    "skill_expansion": True,
                    "dual_role": True,
                    "different_technology": True,
                    "keywords": ["cross-training", "skill expansion", "dual role"]
                },
                "context": "Network admin cross-trained for database support, now primary DBA but servers show network team ownership",
                "indicators": [
                    "User from different technical domain",
                    "Database-specific task performance",
                    "New tool installations",
                    "Original team still listed as owner"
                ]
            },
            "scenario_8": {
                "name": "Vendor Transition - Insourcing",
                "pattern": {
                    "vendor_to_internal": True,
                    "vendor_account": True,
                    "ownership_transfer": True,
                    "keywords": ["vendor", "insourcing", "transition"]
                },
                "context": "External vendor contract ended, management brought in-house but vendor still listed as owner",
                "indicators": [
                    "Vendor account no longer active",
                    "Internal staff performing all tasks",
                    "Migration to internal systems",
                    "Implementation of internal policies"
                ]
            },
            "scenario_9": {
                "name": "Intern to Full-Time Conversion",
                "pattern": {
                    "intern_conversion": True,
                    "restricted_to_full": True,
                    "account_upgrade": True,
                    "keywords": ["intern", "conversion", "full-time hire"]
                },
                "context": "Summer intern hired as full-time QA Engineer, now responsible for test servers but shows intern restrictions",
                "indicators": [
                    "Account changed from .intern suffix",
                    "Full admin access granted",
                    "Production-like environment management",
                    "CI/CD pipeline configuration"
                ]
            },
            "scenario_10": {
                "name": "Merger and Acquisition Integration",
                "pattern": {
                    "company_merger": True,
                    "account_migration": True,
                    "system_integration": True,
                    "keywords": ["merger", "acquisition", "integration"]
                },
                "context": "Acquired company developer now managing combined platform but ownership shows old company credentials",
                "indicators": [
                    "Account domain changed",
                    "Integration with parent company tools",
                    "New security policy compliance",
                    "Collaboration with new teams"
                ]
            },
            "scenario_11": {
                "name": "Sabbatical Return - Role Change",
                "pattern": {
                    "extended_absence": True,
                    "role_evolution": True,
                    "responsibility_shift": True,
                    "keywords": ["sabbatical", "return", "role change"]
                },
                "context": "Architect returned from sabbatical with new AI/ML focus, but previous general systems still show as owner",
                "indicators": [
                    "Long gap in user activity",
                    "Different user now managing systems",
                    "Focus shift to specialized area",
                    "Previous responsibilities not updated"
                ]
            },
            "scenario_12": {
                "name": "Emergency Coverage Becoming Permanent",
                "pattern": {
                    "temporary_to_permanent": True,
                    "medical_coverage": True,
                    "extended_assignment": True,
                    "keywords": ["emergency", "coverage", "permanent"]
                },
                "context": "Temporary coverage during medical emergency became permanent, but CMDB shows original owner",
                "indicators": [
                    "Original owner on extended leave",
                    "Covering user performing all tasks",
                    "Long-term planning activities",
                    "Training others on systems"
                ]
            },
            "scenario_13": {
                "name": "Agile Team Restructuring",
                "pattern": {
                    "methodology_change": True,
                    "cross_functional": True,
                    "product_teams": True,
                    "keywords": ["agile", "restructuring", "product team"]
                },
                "context": "Move to agile methodology with product-based teams, but infrastructure shows old technology-based ownership",
                "indicators": [
                    "Team structure changed to products",
                    "Full-stack responsibilities",
                    "Cross-technology management",
                    "Old team structure obsolete"
                ]
            },
            "scenario_14": {
                "name": "Compliance Officer Role Addition",
                "pattern": {
                    "role_expansion": True,
                    "compliance_focus": True,
                    "additional_responsibility": True,
                    "keywords": ["compliance", "role addition", "security"]
                },
                "context": "Security analyst given compliance responsibilities including servers, previously managed by legal IT",
                "indicators": [
                    "New compliance-focused activities",
                    "Security tool implementations",
                    "Audit and reporting tasks",
                    "Previous owner inactive on CI"
                ]
            },
            "scenario_15": {
                "name": "Outsourcing Reversal",
                "pattern": {
                    "outsource_to_internal": True,
                    "service_transition": True,
                    "external_to_internal": True,
                    "keywords": ["outsourcing", "reversal", "internal"]
                },
                "context": "Outsourced email management brought back in-house, but external provider still listed as owner",
                "indicators": [
                    "External account deactivated",
                    "Internal staff managing services",
                    "Migration to internal tools",
                    "Direct user support provided"
                ]
            },
            "scenario_16": {
                "name": "Graduate Program Rotation",
                "pattern": {
                    "trainee_progression": True,
                    "rotation_complete": True,
                    "permanent_assignment": True,
                    "keywords": ["graduate", "rotation", "permanent"]
                },
                "context": "Graduate trainee completed rotation and permanently assigned, but servers show trainee pool ownership",
                "indicators": [
                    "User no longer in trainee program",
                    "Independent management activities",
                    "Mentoring new trainees",
                    "Long-term planning involvement"
                ]
            },
            "scenario_17": {
                "name": "Startup Acquisition - Founder Integration",
                "pattern": {
                    "acquisition_integration": True,
                    "founder_role": True,
                    "executive_assignment": True,
                    "keywords": ["startup", "acquisition", "founder"]
                },
                "context": "Startup founder now CTO of Innovation Lab, managing acquired servers under new structure",
                "indicators": [
                    "Executive role assignment",
                    "Strategic technology planning",
                    "Innovation initiatives",
                    "Old startup credentials active"
                ]
            },
            "scenario_18": {
                "name": "Shared Service Center Formation",
                "pattern": {
                    "centralization": True,
                    "regional_consolidation": True,
                    "shared_services": True,
                    "keywords": ["shared service", "consolidation", "centralization"]
                },
                "context": "Regional IT teams consolidated into shared service center, but servers show regional ownership",
                "indicators": [
                    "Multiple region management",
                    "Standardization activities",
                    "Cross-regional optimization",
                    "Old regional teams dissolved"
                ]
            },
            "scenario_19": {
                "name": "DevOps Transformation",
                "pattern": {
                    "methodology_shift": True,
                    "devops_adoption": True,
                    "role_convergence": True,
                    "keywords": ["devops", "transformation", "automation"]
                },
                "context": "Traditional ops engineer transitioned to DevOps, managing infrastructure as code",
                "indicators": [
                    "Infrastructure as code implementation",
                    "Automated deployments",
                    "CI/CD integration",
                    "Cross-functional activities"
                ]
            },
            "scenario_20": {
                "name": "Remote Work Policy - Home Office",
                "pattern": {
                    "remote_transition": True,
                    "equipment_reassignment": True,
                    "home_office": True,
                    "keywords": ["remote", "home office", "equipment"]
                },
                "context": "Developer set up home office with company equipment, now primary admin but shows equipment pool ownership",
                "indicators": [
                    "Equipment location changed",
                    "Individual user administration",
                    "Remote access configurations",
                    "Pool manager inactive"
                ]
            },
            "scenario_21": {
                "name": "Emergency Response Team Formation",
                "pattern": {
                    "incident_response": True,
                    "new_team_formation": True,
                    "critical_assignment": True,
                    "keywords": ["emergency", "response team", "security"]
                },
                "context": "Network specialist assigned to new Emergency Response Team, managing security infrastructure",
                "indicators": [
                    "24/7 monitoring activities",
                    "Incident response procedures",
                    "Emergency configurations",
                    "Previous team still listed"
                ]
            },
            "scenario_22": {
                "name": "AI/ML Initiative Assignment",
                "pattern": {
                    "specialized_assignment": True,
                    "gpu_infrastructure": True,
                    "research_computing": True,
                    "keywords": ["AI/ML", "GPU", "research"]
                },
                "context": "Data scientist assigned dedicated GPU servers for ML, but shows generic research computing ownership",
                "indicators": [
                    "ML framework installations",
                    "GPU optimization activities",
                    "Data pipeline configuration",
                    "Generic account listed as owner"
                ]
            },
            "scenario_23": {
                "name": "Quality Assurance Lead Transition",
                "pattern": {
                    "promotion": True,
                    "leadership_transition": True,
                    "inherited_infrastructure": True,
                    "keywords": ["QA lead", "promotion", "transition"]
                },
                "context": "QA Analyst promoted to Lead, inherited test infrastructure from transferred lead",
                "indicators": [
                    "New leadership responsibilities",
                    "Test environment management",
                    "CI/CD pipeline integration",
                    "Previous lead transferred"
                ]
            },
            "scenario_24": {
                "name": "Disaster Recovery Specialist",
                "pattern": {
                    "specialization": True,
                    "dr_focus": True,
                    "dedicated_assignment": True,
                    "keywords": ["disaster recovery", "specialist", "backup"]
                },
                "context": "Sysadmin specialized in DR, assigned backup servers but shows general team ownership",
                "indicators": [
                    "DR procedure implementation",
                    "Backup system optimization",
                    "Recovery testing activities",
                    "General team inactive on CIs"
                ]
            },
            "scenario_25": {
                "name": "Customer Success Platform Migration",
                "pattern": {
                    "platform_migration": True,
                    "customer_focus": True,
                    "ownership_transfer": True,
                    "keywords": ["customer success", "migration", "platform"]
                },
                "context": "Customer Success Manager took over customer portal servers during migration",
                "indicators": [
                    "Customer experience optimization",
                    "Portal customization activities",
                    "Customer tool integration",
                    "Web team no longer active"
                ]
            }
        }
        
        return scenarios
    
    def match_scenario(self, features: Dict, audit_patterns: List[Dict]) -> Tuple[str, float, str]:
        """
        Match features and audit patterns to most likely scenario
        Returns: (scenario_id, confidence, context)
        """
        best_match = None
        best_score = 0
        best_context = ""
        
        for scenario_id, scenario in self.scenarios.items():
            score = self._calculate_match_score(features, audit_patterns, scenario)
            
            if score > best_score:
                best_score = score
                best_match = scenario_id
                best_context = scenario['context']
        
        return best_match, best_score, best_context
    
    def _calculate_match_score(self, features: Dict, audit_patterns: List[Dict], scenario: Dict) -> float:
        """Calculate how well features match a scenario"""
        score = 0.0
        pattern = scenario['pattern']
        
        # Check pattern matches
        if pattern.get('role_change') and features.get('role_changed', False):
            score += 0.2
        
        if pattern.get('new_user') and features.get('account_age_days', 999) < 90:
            score += 0.2
            
        if pattern.get('terminated_owner') and features.get('account_terminated', False):
            score += 0.3
            
        if pattern.get('vendor_account') and features.get('vendor_account_indicator', False):
            score += 0.2
            
        if pattern.get('multiple_users_active') and features.get('multiple_users_active', False):
            score += 0.15
            
        if pattern.get('org_change') and features.get('organizational_changes', 0) > 0:
            score += 0.15
            
        # Check keyword matches in audit patterns
        keywords_found = 0
        for keyword in pattern.get('keywords', []):
            for audit in audit_patterns:
                if keyword.lower() in str(audit).lower():
                    keywords_found += 1
                    break
        
        if len(pattern.get('keywords', [])) > 0:
            keyword_score = keywords_found / len(pattern.get('keywords', []))
            score += keyword_score * 0.2
        
        # Temporal pattern matching
        if pattern.get('extended_absence') and features.get('days_since_owner_activity', 0) > 180:
            score += 0.2
            
        if pattern.get('contractor_pattern') and '.contractor' in str(features.get('assigned_owner', '')):
            score += 0.15
            
        return min(score, 1.0)  # Cap at 1.0


class EnhancedStalenessDetector:
    """
    Enhanced staleness detector with scenario matching and owner recommendation
    """
    
    def __init__(self):
        self.kb = ScenarioKnowledgeBase()
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
        """Train the model by learning patterns from labeled data"""
        print("Training enhanced staleness detector with scenario matching...")
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
        """Extract and categorize patterns from the training data"""
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
            
            # If stale, match to scenario
            matched_scenario = None
            scenario_confidence = 0
            scenario_context = ""
            
            if label.get('staleness_label') == 1:
                matched_scenario, scenario_confidence, scenario_context = self.kb.match_scenario(
                    features, audit_records
                )
            
            scenario_data.append({
                'features': features,
                'staleness_reasons': label.get('staleness_reasons', ''),
                'staleness_label': label.get('staleness_label'),
                'staleness_score': label.get('staleness_score', 0),
                'confidence': label.get('confidence', 0),
                'ci_id': ci_id,
                'assigned_owner': label.get('assigned_owner', ''),
                'matched_scenario': matched_scenario,
                'scenario_confidence': scenario_confidence,
                'scenario_context': scenario_context
            })
        
        print(f"Extracted {len(scenario_data)} scenario instances")
        return scenario_data
    
    def _learn_scenario_patterns(self, scenario_data):
        """Learn patterns from scenario data"""
        print("Learning scenario patterns...")
        
        # Group scenarios by similar patterns
        reason_groups = defaultdict(list)
        
        for data in scenario_data:
            if data['staleness_label'] == 1 and data['staleness_reasons']:
                normalized_reason = self._normalize_reason_string(data['staleness_reasons'])
                reason_groups[normalized_reason].append(data)
        
        # Create scenario patterns
        scenario_id = 1
        for reason_pattern, instances in reason_groups.items():
            if len(instances) >= 1:
                common_features = self._extract_common_features(instances)
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
    
    def predict_single(self, ci_data):
        """Predict staleness for a single CI with scenario matching and owner recommendation"""
        if not self.trained:
            raise ValueError("Model must be trained before making predictions")
        
        # Extract features
        features = self._extract_all_features(ci_data)
        
        # Match against learned scenarios
        scenario_matches = []
        max_confidence = 0
        
        for scenario_id, scenario_pattern in self.scenario_patterns.items():
            match_score = self._calculate_scenario_match_score(features, scenario_pattern)
            
            if match_score > 0.3:
                confidence = min(0.98, scenario_pattern['avg_confidence'] * match_score)
                scenario_matches.append({
                    'scenario_id': scenario_id,
                    'match_score': match_score,
                    'confidence': confidence,
                    'reason_pattern': scenario_pattern['reason_pattern'],
                    'original_reasons': scenario_pattern['original_reasons'][0]
                })
                max_confidence = max(max_confidence, confidence)
        
        # Sort by match score
        scenario_matches.sort(key=lambda x: x['match_score'], reverse=True)
        
        # Determine staleness
        is_stale = max_confidence > 0.7
        
        # If stale, match to KB scenario for context
        kb_scenario = None
        kb_confidence = 0
        kb_context = ""
        
        if is_stale:
            audit_records = ci_data.get('audit_records', [])
            kb_scenario, kb_confidence, kb_context = self.kb.match_scenario(features, audit_records)
        
        # Generate explanation combining learned patterns and KB scenarios
        if kb_scenario and kb_confidence > 0.1:  # Lower threshold to use KB scenarios
            explanation = self._substitute_context_with_users(kb_context, ci_data)
        elif scenario_matches:
            best_match = scenario_matches[0]
            explanation = self._generate_explanation(features, best_match)
        else:
            explanation = "No matching scenario patterns found"
        
        # Recommend new owners
        new_owner_recommendations = self._recommend_new_owners(ci_data)
        
        return {
            'is_stale': is_stale,
            'confidence': max_confidence,
            'matched_scenarios': scenario_matches[:3],
            'kb_scenario': kb_scenario,
            'kb_scenario_name': self.kb.scenarios[kb_scenario]['name'] if kb_scenario else None,
            'kb_confidence': kb_confidence,
            'explanation': explanation,
            'new_owner_recommendations': new_owner_recommendations,
            'features': features
        }
    
    def _recommend_new_owners(self, ci_data):
        """Recommend potential new owners based on activity patterns"""
        audit_records = ci_data.get('audit_records', [])
        assigned_owner = ci_data.get('assigned_owner', '')
        
        if not audit_records:
            return []
        
        # Analyze user activities (excluding current owner and system)
        user_scores = {}
        
        for record in audit_records:
            user = record.get('user', '')
            if user and user != assigned_owner and user != 'system':
                if user not in user_scores:
                    user_scores[user] = {
                        'activity_count': 0,
                        'last_activity': None,
                        'ownership_changes': 0,
                        'critical_changes': 0,
                        'recent_activities': 0
                    }
                
                user_scores[user]['activity_count'] += 1
                
                # Track ownership field changes
                if str(record.get('fieldname', '')).lower() in ['assigned_to', 'managed_by', 'support_group']:
                    user_scores[user]['ownership_changes'] += 1
                
                # Track critical field changes
                if str(record.get('fieldname', '')).lower() in ['cpu_count', 'ram', 'os_version', 'configuration']:
                    user_scores[user]['critical_changes'] += 1
                
                # Track recent activities (last 30 days)
                try:
                    activity_date = self._parse_date(record.get('sys_created_on', ''))
                    if (datetime.now() - activity_date).days <= 30:
                        user_scores[user]['recent_activities'] += 1
                    
                    if user_scores[user]['last_activity'] is None or activity_date > user_scores[user]['last_activity']:
                        user_scores[user]['last_activity'] = activity_date
                except:
                    pass
        
        # Calculate confidence scores for each potential owner
        recommendations = []
        total_activities = len(audit_records)
        
        for user, data in user_scores.items():
            # Calculate confidence score based on multiple factors
            confidence = 0.0
            
            # Activity volume (30%)
            activity_ratio = data['activity_count'] / total_activities
            confidence += min(activity_ratio * 0.3, 0.3)
            
            # Recency (25%)
            if data['last_activity']:
                days_since = (datetime.now() - data['last_activity']).days
                recency_score = max(0, (30 - days_since) / 30)
                confidence += recency_score * 0.25
            
            # Ownership changes (20%)
            if data['ownership_changes'] > 0:
                confidence += 0.20
            
            # Critical changes (15%)
            if data['critical_changes'] > 0:
                confidence += 0.15
            
            # Recent activity intensity (10%)
            if data['recent_activities'] > 5:
                confidence += 0.10
            
            recommendations.append({
                'user': user,
                'confidence': min(confidence, 1.0),
                'activity_count': data['activity_count'],
                'recent_activities': data['recent_activities'],
                'ownership_changes': data['ownership_changes'],
                'last_activity_days_ago': (datetime.now() - data['last_activity']).days if data['last_activity'] else 999
            })
        
        # Sort by confidence and return top 5
        recommendations.sort(key=lambda x: x['confidence'], reverse=True)
        return recommendations[:5]
    
    def _normalize_reason_string(self, reason_string):
        """Normalize reason strings to group similar patterns"""
        if not reason_string or pd.isna(reason_string):
            return ""
        
        parts = [part.strip() for part in str(reason_string).split(';')]
        
        normalized_parts = []
        for part in parts:
            part = re.sub(r'\d+ days', 'X days', part)
            part = re.sub(r'>\d+ months?', '>X months', part)
            part = re.sub(r'>\d+ years?', '>X years', part)
            normalized_parts.append(part)
        
        return '; '.join(sorted(normalized_parts))
    
    def _extract_common_features(self, instances):
        """Extract features that are common across instances"""
        if not instances:
            return {}
        
        all_features = set()
        for inst in instances:
            all_features.update(inst['features'].keys())
        
        common_features = {}
        for feature in all_features:
            values = [inst['features'].get(feature) for inst in instances if feature in inst['features']]
            
            if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in values if v is not None):
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
                values = [v for v in values if v is not None]
                if values:
                    common_features[feature] = {
                        'type': 'boolean',
                        'true_ratio': sum(values) / len(values)
                    }
            else:
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
        """Calculate thresholds for feature matching"""
        thresholds = {}
        
        for inst in instances:
            for feature, value in inst['features'].items():
                if isinstance(value, (int, float)) and not isinstance(value, bool) and feature not in thresholds:
                    all_values = [i['features'].get(feature) for i in instances if feature in i['features']]
                    all_values = [v for v in all_values if isinstance(v, (int, float)) and not isinstance(v, bool)]
                    
                    if all_values:
                        thresholds[feature] = {
                            'min_threshold': min(all_values) * 0.8,
                            'max_threshold': max(all_values) * 1.2,
                            'mean': np.mean(all_values)
                        }
        
        return thresholds
    
    def _calculate_feature_weights(self, instances):
        """Calculate importance weights for features"""
        weights = {}
        
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
        
        for inst in instances:
            for feature in inst['features'].keys():
                weights[feature] = reason_features.get(feature, 1.0)
        
        return weights
    
    def _extract_all_features(self, ci_data):
        """Extract all features using the defined extractors"""
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
        
        # Department and role changes
        audit_records = ci_data.get('audit_records', [])
        org_change_fields = ['department', 'title', 'role', 'manager', 'location']
        org_changes = sum(1 for r in audit_records if str(r.get('fieldname', '')).lower() in org_change_fields)
        features['organizational_changes'] = org_changes
        features['has_organizational_changes'] = org_changes > 0
        
        # Check for role changes
        if user_info:
            features['role_changed'] = False
            for record in audit_records:
                if record.get('fieldname') == 'title' and record.get('user') == assigned_owner:
                    features['role_changed'] = True
                    break
        
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
        
        # Recent activity analysis
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
    
    def _calculate_scenario_match_score(self, features, scenario_pattern):
        """Calculate how well features match a scenario pattern"""
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
                    if feature_name in thresholds:
                        threshold_data = thresholds[feature_name]
                        if threshold_data['min_threshold'] <= feature_value <= threshold_data['max_threshold']:
                            score = 1.0
                        else:
                            distance = min(
                                abs(feature_value - threshold_data['min_threshold']),
                                abs(feature_value - threshold_data['max_threshold'])
                            )
                            max_distance = max(threshold_data['max_threshold'] - threshold_data['min_threshold'], 1)
                            score = max(0, 1.0 - (distance / max_distance))
                    else:
                        score = 0.5
                
                elif feature_data['type'] == 'boolean':
                    if isinstance(feature_value, bool):
                        expected_ratio = feature_data['true_ratio']
                        if (feature_value and expected_ratio > 0.5) or (not feature_value and expected_ratio <= 0.5):
                            score = 1.0
                        else:
                            score = 0.0
                    else:
                        score = 0.5
                
                else:  # categorical
                    most_common_values = [item[0] for item in feature_data['most_common']]
                    if str(feature_value) in most_common_values:
                        score = 1.0
                    else:
                        score = 0.0
                
                weighted_score += score * weight
        
        return weighted_score / total_weight if total_weight > 0 else 0
    
    def _substitute_context_with_users(self, kb_context, ci_data):
        """Substitute generic terms in KB context with actual user names"""
        assigned_owner = ci_data.get('assigned_owner', '')
        audit_records = ci_data.get('audit_records', [])
        
        # Get the most active non-owner user
        other_users = {}
        for record in audit_records:
            user = record.get('user', '')
            if user and user != assigned_owner and user != 'system':
                other_users[user] = other_users.get(user, 0) + 1
        
        most_active_user = max(other_users.keys(), key=lambda x: other_users[x]) if other_users else "Unknown User"
        
        # Get manager/supervisor from user info if available
        user_info = ci_data.get('user_info', {})
        manager = user_info.get('manager', 'the manager')
        
        # Substitute generic terms with actual names
        context = kb_context
        context = context.replace('New DBA', most_active_user)
        context = context.replace('Junior developer', most_active_user) 
        context = context.replace('Person C', most_active_user)
        context = context.replace('manager', manager if manager != 'the manager' else assigned_owner)
        context = context.replace('his manager', manager if manager != 'the manager' else assigned_owner)
        context = context.replace('previous lead', assigned_owner)
        context = context.replace('IT Manager', assigned_owner)
        context = context.replace('team members', most_active_user)
        
        return context

    def _generate_explanation(self, features, best_match):
        """Generate human-readable explanation"""
        explanations = []
        
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


def split_and_save_data(labels_df, output_dir='./data_splits/'):
    """
    Split data into train (70%), validation (20%), and test (10%) sets
    Save each split to a separate CSV file
    """
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    print("Splitting data into train/validation/test sets...")
    
    # First split: separate train (70%) from temp (30%)
    X = labels_df.drop('staleness_label', axis=1) if 'staleness_label' in labels_df.columns else labels_df
    y = labels_df['staleness_label'] if 'staleness_label' in labels_df.columns else pd.Series([0] * len(labels_df))
    
    X_train, X_temp, y_train, y_temp = train_test_split(
        X, y, 
        test_size=0.3, 
        random_state=42, 
        stratify=y
    )
    
    # Second split: separate validation (20%) from test (10%)
    # 20% of total = 2/3 of the 30% temp set
    X_val, X_test, y_val, y_test = train_test_split(
        X_temp, y_temp,
        test_size=0.333,  # 10% / 30% = 0.333
        random_state=42,
        stratify=y_temp
    )
    
    # Recreate DataFrames with labels
    train_df = X_train.copy()
    train_df['staleness_label'] = y_train
    
    val_df = X_val.copy()
    val_df['staleness_label'] = y_val
    
    test_df = X_test.copy()
    test_df['staleness_label'] = y_test
    
    # Save to files
    train_df.to_csv(f'{output_dir}train_data.csv', index=False)
    val_df.to_csv(f'{output_dir}validation_data.csv', index=False)
    test_df.to_csv(f'{output_dir}test_data.csv', index=False)
    
    print(f"Data split completed and saved to {output_dir}")
    print(f"  Training set: {len(train_df)} samples ({len(train_df)/len(labels_df)*100:.1f}%)")
    print(f"    - Stale: {sum(y_train)} ({sum(y_train)/len(y_train)*100:.1f}%)")
    print(f"    - Non-stale: {len(y_train) - sum(y_train)} ({(len(y_train) - sum(y_train))/len(y_train)*100:.1f}%)")
    
    print(f"  Validation set: {len(val_df)} samples ({len(val_df)/len(labels_df)*100:.1f}%)")
    print(f"    - Stale: {sum(y_val)} ({sum(y_val)/len(y_val)*100:.1f}%)")
    print(f"    - Non-stale: {len(y_val) - sum(y_val)} ({(len(y_val) - sum(y_val))/len(y_val)*100:.1f}%)")
    
    print(f"  Test set: {len(test_df)} samples ({len(test_df)/len(labels_df)*100:.1f}%)")
    print(f"    - Stale: {sum(y_test)} ({sum(y_test)/len(y_test)*100:.1f}%)")
    print(f"    - Non-stale: {len(y_test) - sum(y_test)} ({(len(y_test) - sum(y_test))/len(y_test)*100:.1f}%)")
    
    return train_df, val_df, test_df


def prepare_test_data(test_df, audit_df, user_df, ci_df):
    """Prepare test data for prediction"""
    print("Preparing test data...")
    
    # Build lookup dictionaries with duplicate handling
    audit_by_ci = audit_df.groupby('documentkey').apply(lambda x: x.to_dict('records')).to_dict()
    
    user_df_dedup = user_df.drop_duplicates(subset=['user_name'], keep='last')
    user_by_name = user_df_dedup.set_index('user_name').to_dict('index')
    
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


def evaluate_enhanced_model(detector, test_data):
    """Evaluate the enhanced model with scenario matching"""
    print("Evaluating enhanced model on test data...")
    
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
            'kb_scenario': result.get('kb_scenario_name', 'None'),
            'kb_confidence': result.get('kb_confidence', 0),
            'matched_scenarios': len(result['matched_scenarios']),
            'top_recommended_owners': result.get('new_owner_recommendations', [])[:3]
        })
    
    # Calculate metrics
    accuracy = accuracy_score(y_true, y_pred)
    precision, recall, f1, support = precision_recall_fscore_support(y_true, y_pred, average='weighted')
    
    print(f"\n📊 Enhanced Model Evaluation Results:")
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
    plt.title('Confusion Matrix - Enhanced Model')
    plt.ylabel('Actual Label')
    plt.xlabel('Predicted Label')
    plt.show()
    
    # Show some example predictions with scenarios
    print(f"\n🔍 Sample Predictions with Scenario Matching:")
    stale_results = [r for r in detailed_results if r['predicted_label'] == 1][:5]
    
    for i, result in enumerate(stale_results):
        print(f"\n{i+1}. CI: {result['ci_id']}")
        print(f"   Confidence: {result['confidence']:.3f}")
        print(f"   Scenario: {result['kb_scenario']}")
        print(f"   Explanation: {result['predicted_explanation']}")
        
        if result['top_recommended_owners']:
            print(f"   Recommended New Owners:")
            for j, owner in enumerate(result['top_recommended_owners']):
                print(f"     {j+1}. {owner['user']} (Confidence: {owner['confidence']:.2f}, "
                      f"Activities: {owner['activity_count']}, "
                      f"Last Active: {owner['last_activity_days_ago']} days ago)")
    
    return {
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'confusion_matrix': cm,
        'detailed_results': detailed_results
    }


# Main execution
def main():
    """Main function to run the enhanced staleness detector"""
    print("Enhanced Staleness Detector with Scenario Knowledge Base")
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
        print("Please ensure all CSV files are in the current directory")
        return
    
    # Split and save the data
    print("\n" + "=" * 80)
    train_df, val_df, test_df = split_and_save_data(labels_df)
    
    # Create and train the enhanced model
    print("\n" + "=" * 80)
    detector = EnhancedStalenessDetector()
    detector.train_from_data(train_df, audit_df, user_df, ci_df)
    
    # Save the trained model
    model_filename = 'enhanced_staleness_model_with_kb.pkl'
    detector.save_model(model_filename)
    
    # Get training summary
    summary = detector.get_training_summary()
    print(f"\n📊 Training Summary:")
    print(f"Total scenarios learned: {summary['total_scenarios']}")
    print(f"Training CIs: {summary['training_stats']['total_training_cis']}")
    print(f"  - Stale: {summary['training_stats']['stale_training_cis']}")
    print(f"  - Non-stale: {summary['training_stats']['non_stale_training_cis']}")
    
    # Evaluate on validation data
    print("\n" + "=" * 80)
    print("Evaluating on validation data...")
    val_data = prepare_test_data(val_df, audit_df, user_df, ci_df)
    val_results = evaluate_enhanced_model(detector, val_data)
    
    # Evaluate on test data
    print("\n" + "=" * 80)
    print("Evaluating on test data...")
    test_data = prepare_test_data(test_df, audit_df, user_df, ci_df)
    test_results = evaluate_enhanced_model(detector, test_data)
    
    # Generate comprehensive report for stale CIs
    print("\n" + "=" * 80)
    print("Generating Stale CI Report with Scenarios and Recommendations...")
    
    # Process all data to find stale CIs
    all_data = prepare_test_data(labels_df, audit_df, user_df, ci_df)
    stale_ci_report = []
    
    for item in all_data:
        result = detector.predict_single(item['ci_data'])
        
        if result['is_stale']:
            ci_info = {
                'ci_id': item['ci_id'],
                'assigned_owner': item['ci_data']['assigned_owner'],
                'confidence': result['confidence'],
                'scenario': result.get('kb_scenario_name', 'Unknown'),
                'scenario_confidence': result.get('kb_confidence', 0),
                'explanation': result['explanation'],
                'recommended_owners': []
            }
            
            # Add recommended owners
            for owner in result.get('new_owner_recommendations', [])[:3]:
                ci_info['recommended_owners'].append({
                    'user': owner['user'],
                    'confidence': owner['confidence'],
                    'activity_count': owner['activity_count'],
                    'last_activity_days_ago': owner['last_activity_days_ago']
                })
            
            stale_ci_report.append(ci_info)
    
    # Save stale CI report
    stale_ci_df = pd.DataFrame(stale_ci_report)
    stale_ci_df.to_csv('stale_ci_report_with_scenarios.csv', index=False)
    print(f"\n✅ Found {len(stale_ci_report)} stale CIs out of {len(all_data)} total CIs")
    print(f"Stale CI report saved to: stale_ci_report_with_scenarios.csv")
    
    # Print scenario distribution
    scenario_counts = {}
    for ci in stale_ci_report:
        scenario = ci['scenario']
        scenario_counts[scenario] = scenario_counts.get(scenario, 0) + 1
    
    print("\n📊 Scenario Distribution for Stale CIs:")
    sorted_scenarios = sorted(scenario_counts.items(), key=lambda x: x[1], reverse=True)
    for scenario, count in sorted_scenarios[:10]:
        print(f"  {scenario}: {count} CIs ({count/len(stale_ci_report)*100:.1f}%)")
    
    print(f"\n✅ Enhanced model training and evaluation completed successfully!")
    print(f"Model saved as: {model_filename}")
    
    print(f"\n📈 Final Results Summary:")
    print(f"Validation Set Performance:")
    print(f"  - Accuracy: {val_results['accuracy']:.3f}")
    print(f"  - F1-Score: {val_results['f1']:.3f}")
    print(f"Test Set Performance:")
    print(f"  - Accuracy: {test_results['accuracy']:.3f}")
    print(f"  - F1-Score: {test_results['f1']:.3f}")
    
    return detector, val_results, test_results, stale_ci_report


# Function to generate detailed stale CI analysis
def generate_detailed_stale_analysis(detector, labels_df, audit_df, user_df, ci_df, output_file='detailed_stale_analysis.csv'):
    """Generate a detailed analysis of all stale CIs with scenarios and recommendations"""
    
    print("\nGenerating detailed stale CI analysis...")
    
    # Prepare data
    all_data = prepare_test_data(labels_df, audit_df, user_df, ci_df)
    detailed_analysis = []
    
    for item in all_data:
        result = detector.predict_single(item['ci_data'])
        
        if result['is_stale']:
            # Get CI details
            ci_id = item['ci_id']
            ci_info = item['ci_data']['ci_info']
            
            analysis = {
                'ci_id': ci_id,
                'ci_name': ci_info.get('name', ''),
                'ci_type': ci_info.get('sys_class_name', ''),
                'current_owner': item['ci_data']['assigned_owner'],
                'staleness_confidence': result['confidence'],
                'risk_level': 'Critical' if result['confidence'] > 0.9 else 'High' if result['confidence'] > 0.8 else 'Medium',
                'matched_scenario': result.get('kb_scenario_name', 'Unknown'),
                'scenario_confidence': result.get('kb_confidence', 0),
                'explanation': result['explanation'],
                'days_since_owner_activity': result['features'].get('days_since_owner_activity', 999),
                'total_activities': result['features'].get('total_activity_count', 0),
                'owner_activity_count': result['features'].get('owner_activity_count', 0),
                'other_users_count': result['features'].get('other_users_count', 0)
            }
            
            # Add top 3 recommended owners
            for i, owner in enumerate(result.get('new_owner_recommendations', [])[:3]):
                analysis[f'recommended_owner_{i+1}'] = owner['user']
                analysis[f'recommended_owner_{i+1}_confidence'] = owner['confidence']
                analysis[f'recommended_owner_{i+1}_activities'] = owner['activity_count']
                analysis[f'recommended_owner_{i+1}_last_active_days'] = owner['last_activity_days_ago']
            
            detailed_analysis.append(analysis)
    
    # Create DataFrame and save
    analysis_df = pd.DataFrame(detailed_analysis)
    analysis_df = analysis_df.sort_values('staleness_confidence', ascending=False)
    analysis_df.to_csv(output_file, index=False)
    
    print(f"✅ Detailed analysis saved to: {output_file}")
    print(f"   Total stale CIs analyzed: {len(detailed_analysis)}")
    
    return analysis_df


# Function to visualize scenario distribution
def visualize_scenario_distribution(stale_ci_report):
    """Create visualizations for scenario distribution"""
    
    # Count scenarios
    scenario_counts = {}
    for ci in stale_ci_report:
        scenario = ci['scenario']
        scenario_counts[scenario] = scenario_counts.get(scenario, 0) + 1
    
    # Create bar plot
    plt.figure(figsize=(12, 8))
    scenarios = list(scenario_counts.keys())
    counts = list(scenario_counts.values())
    
    # Sort by count
    sorted_data = sorted(zip(scenarios, counts), key=lambda x: x[1], reverse=True)
    scenarios, counts = zip(*sorted_data[:15])  # Top 15 scenarios
    
    plt.barh(scenarios, counts, color='steelblue')
    plt.xlabel('Number of CIs')
    plt.ylabel('Scenario')
    plt.title('Top 15 Scenario Distribution for Stale CIs')
    plt.tight_layout()
    plt.show()
    
    # Create pie chart for top scenarios
    plt.figure(figsize=(10, 8))
    top_scenarios = sorted_data[:10]
    other_count = sum(count for _, count in sorted_data[10:])
    
    labels = [s[0] for s in top_scenarios] + ['Others']
    sizes = [s[1] for s in top_scenarios] + [other_count]
    
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    plt.title('Scenario Distribution for Stale CIs')
    plt.axis('equal')
    plt.tight_layout()
    plt.show()


# Run the main function
if __name__ == "__main__":
    detector, val_results, test_results, stale_ci_report = main()
    
    # Generate detailed analysis
    detailed_analysis = generate_detailed_stale_analysis(
        detector,
        pd.read_csv('cmdb_staleness_labels.csv'),
        pd.read_csv('updated_sys_audit.csv'),
        pd.read_csv('synthetic_sys_user.csv'),
        pd.read_csv('synthetic_cmdb_ci.csv')
    )
    
    # Visualize results
    visualize_scenario_distribution(stale_ci_report)