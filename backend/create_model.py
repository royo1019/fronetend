import pandas as pd
import numpy as np
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

class RuleBasedStalenessDetector:
    """
    Pickle-serializable version of the staleness detector
    """

    def __init__(self):
        self.rules = self._define_detection_rules()
        self.scenario_patterns = self._define_scenario_patterns()

    def _define_detection_rules(self):
        """Define rules based on the document patterns"""
        return {
            'inactive_owner_active_others': {
                'description': 'Owner has 0 activities while others are active',
                'conditions': [
                    'owner_activity_count == 0',
                    'total_activity_count >= 2',
                    'other_users_count > 0'
                ],
                'confidence': 0.95,
                'scenarios': ['1', '5', '11']
            },
            'account_terminated': {
                'description': 'Owner account is inactive/terminated',
                'conditions': [
                    'owner_active == False'
                ],
                'confidence': 1.0,
                'scenarios': ['5', '8']
            },
            'vendor_account': {
                'description': 'Assigned to vendor/external account',
                'conditions': [
                    "'vendor' in owner_name.lower() or 'external' in owner_name.lower() or '.contractor' in owner_name"
                ],
                'confidence': 0.85,
                'scenarios': ['4', '8']
            },
            'generic_account': {
                'description': 'Assigned to generic account',
                'conditions': [
                    "'.generic' in owner_name or 'admin.generic' in owner_name or 'team.generic' in owner_name"
                ],
                'confidence': 0.9,
                'scenarios': ['7', '10', '15']
            },
            'extended_inactivity': {
                'description': 'No owner activity for 150+ days',
                'conditions': [
                    'days_since_owner_activity > 150',
                    'recent_other_activities >= 0'
                ],
                'confidence': 0.85,
                'scenarios': ['1', '9']
            },
            'role_transition': {
                'description': 'User role changed significantly',
                'conditions': [
                    'owner_role_changes > 0',
                    'owner_title_changed == True'
                ],
                'confidence': 0.75,
                'scenarios': ['1', '3', '9', '11']
            },
            'department_transition': {
                'description': 'User moved to different department',
                'conditions': [
                    'owner_dept_changed == True',
                    'days_since_owner_activity > 15'
                ],
                'confidence': 0.8,
                'scenarios': ['3', '6']
            },
            'group_disbanded': {
                'description': 'Assigned group no longer active',
                'conditions': [
                    'assigned_group_active == False'
                ],
                'confidence': 0.9,
                'scenarios': ['6', '13']
            },
            'dominant_other_user': {
                'description': 'Another user has majority of recent activities',
                'conditions': [
                    'top_other_user_ratio > 0.5',
                    'owner_activity_ratio < 0.3'
                ],
                'confidence': 0.85,
                'scenarios': ['2', '7', '12']
            },
            'ownership_field_changes': {
                'description': 'Ownership fields modified by non-owner',
                'conditions': [
                    'non_owner_ownership_changes > 0'
                ],
                'confidence': 0.9,
                'scenarios': ['multiple']
            },
            'minimal_owner_activity': {
                'description': 'Owner has very little recent activity',
                'conditions': [
                    'owner_activity_count <= 1',
                    'total_activity_count > 0'
                ],
                'confidence': 0.7,
                'scenarios': ['general']
            }
        }

    def _define_scenario_patterns(self):
        """Define specific patterns from each scenario"""
        return {
        # Promotion and Role Changes
        'promotion_pattern': {
            'indicators': [
                'title_change_to_lead_or_manager',
                'role_additions_with_elevated_privileges',
                'group_membership_change_to_leadership',
                'increased_activity_by_successor',
                'zero_or_declining_activity_by_promoted_user',
                'responsibility_inheritance_pattern',
                'team_member_becomes_primary_admin'
            ],
            'scenarios': ['1', '23'],
            'detection_queries': {
                'title_change': "fieldname='title' AND newvalue LIKE '%Lead%' OR newvalue LIKE '%Manager%'",
                'role_elevation': "tablename='sys_user_has_role' AND role IN ('team_lead', 'manager', 'admin')"
            }
        },
        
        'onboarding_mismatch': {
            'indicators': [
                'new_user_created_recently',
                'ci_assigned_to_manager_initially',
                'actual_user_different_from_assigned',
                'daily_activities_by_actual_user',
                'setup_activities_by_new_hire',
                'password_changes_by_new_user',
                'software_installations_by_new_user',
                'no_activities_by_assigned_manager'
            ],
            'scenarios': ['2', '9', '16'],
            'detection_queries': {
                'new_hire': "user.hire_date > DATEADD(day, -90, GETDATE())",
                'setup_pattern': "fieldname IN ('password', 'software_install', 'user_profile')"
            }
        },
        
        'department_reorganization': {
            'indicators': [
                'new_group_or_department_created',
                'old_group_deactivated_or_renamed',
                'mass_user_transitions_between_groups',
                'department_field_changes_bulk',
                'ci_group_assignments_updated',
                'reporting_structure_changes',
                'split_team_patterns',
                'consolidated_team_patterns'
            ],
            'scenarios': ['3', '6', '13', '18'],
            'detection_queries': {
                'group_changes': "tablename='sys_user_group' AND active=false",
                'bulk_transitions': "COUNT(DISTINCT user) > 5 AND fieldname='department'"
            }
        },
        
        'external_to_internal': {
            'indicators': [
                'contractor_account_deactivated',
                'new_internal_account_created_same_person',
                'vendor_prefix_in_old_account',
                'contractor_suffix_removal',
                'enhanced_permissions_granted',
                'production_access_enabled',
                'on_call_rotation_added',
                'full_admin_rights_granted'
            ],
            'scenarios': ['4', '8', '15'],
            'detection_queries': {
                'contractor_pattern': "user_name LIKE '%.contractor' OR user_name LIKE 'vendor.%'",
                'permission_upgrade': "role_changes FROM 'contractor' TO 'employee'"
            }
        },
        
        'manager_departure': {
            'indicators': [
                'manager_account_deactivated',
                'terminated_employee_status',
                'orphaned_ci_ownership',
                'multiple_team_members_sharing_work',
                'interim_manager_assigned',
                'escalation_patterns_to_new_manager',
                'distributed_responsibility_pattern',
                'no_single_owner_identified'
            ],
            'scenarios': ['5'],
            'detection_queries': {
                'terminated_user': "user.active=false AND user.termination_date IS NOT NULL",
                'orphaned_cis': "assigned_to IN (SELECT sys_id FROM sys_user WHERE active=false)"
            }
        },
        
        'project_team_dissolution': {
            'indicators': [
                'project_completion_status',
                'team_disbanded_date_set',
                'members_reassigned_to_new_teams',
                'ci_repurposed_for_new_function',
                'project_specific_access_removed',
                'new_business_purpose_assigned',
                'configuration_changes_for_new_use',
                'ownership_transfer_to_permanent_team'
            ],
            'scenarios': ['6'],
            'detection_queries': {
                'disbanded_team': "group.active=false AND group.type='project'",
                'repurposed_ci': "fieldname='business_purpose' AND oldvalue LIKE '%project%'"
            }
        },
        
        'cross_training_transition': {
            'indicators': [
                'user_from_different_team_active',
                'original_team_skill_shortage',
                'cross_functional_activities',
                'dual_team_membership',
                'gradual_responsibility_shift',
                'original_team_reduced_activity',
                'new_skill_certifications_added',
                'permanent_role_reassignment'
            ],
            'scenarios': ['7'],
            'detection_queries': {
                'cross_team_activity': "user.primary_group != ci.support_group",
                'dual_membership': "COUNT(DISTINCT group_membership) > 1"
            }
        },
        
        'vendor_to_inhouse': {
            'indicators': [
                'vendor_contract_end_date_reached',
                'external_account_deactivation',
                'internal_team_takeover',
                'knowledge_transfer_activities',
                'documentation_updates_by_internal',
                'monitoring_tool_migrations',
                'security_policy_implementations',
                'vendor_specific_access_removal'
            ],
            'scenarios': ['8', '15'],
            'detection_queries': {
                'vendor_transition': "user_name LIKE 'vendor.%' AND active=false",
                'internal_takeover': "new_owner.company = 'internal' AND timestamp > vendor.end_date"
            }
        },
        
        'intern_conversion': {
            'indicators': [
                'intern_account_upgraded',
                'intern_suffix_removed',
                'full_time_employee_record_created',
                'enhanced_access_privileges',
                'production_permissions_granted',
                'mentor_supervision_removed',
                'independent_work_assignments',
                'permanent_team_membership'
            ],
            'scenarios': ['9', '16'],
            'detection_queries': {
                'intern_pattern': "user_name LIKE '%.intern' OR user_group = 'interns'",
                'conversion': "new_user.employee_type = 'FTE' AND old_user.employee_type = 'intern'"
            }
        },
        
        'merger_acquisition': {
            'indicators': [
                'acquired_company_suffix_present',
                'account_migration_pending',
                'dual_identity_period',
                'system_integration_activities',
                'policy_alignment_changes',
                'new_corporate_standards_applied',
                'legacy_system_decommission',
                'unified_platform_migration'
            ],
            'scenarios': ['10', '17'],
            'detection_queries': {
                'acquired_pattern': "user_name LIKE '%.xyz' OR user_name LIKE '%.acquired'",
                'integration': "company_transition FROM 'acquired_company' TO 'parent_company'"
            }
        },
        
        'sabbatical_coverage': {
            'indicators': [
                'extended_leave_status_active',
                'leave_start_date_recorded',
                'temporary_coverage_assigned',
                'coverage_becoming_permanent',
                'original_owner_inactive_extended',
                'role_specialization_change',
                'focus_area_narrowed',
                'responsibilities_redistributed'
            ],
            'scenarios': ['11', '12'],
            'detection_queries': {
                'leave_status': "user.leave_status = 'sabbatical' OR user.leave_status = 'medical'",
                'extended_coverage': "coverage_duration > 90 AND coverage_type = 'temporary'"
            }
        },
        
        'emergency_coverage_permanent': {
            'indicators': [
                'emergency_assignment_initial',
                'medical_leave_extended',
                'temporary_becomes_permanent',
                'coverage_user_primary_activities',
                'strategic_planning_by_coverage',
                'team_training_by_coverage',
                'long_term_projects_assigned',
                'original_owner_return_unlikely'
            ],
            'scenarios': ['12'],
            'detection_queries': {
                'medical_leave': "user.leave_type = 'medical' AND leave_duration > 60",
                'permanent_takeover': "coverage_status CHANGED FROM 'temporary' TO 'permanent'"
            }
        },
        
        'agile_transformation': {
            'indicators': [
                'team_structure_flattened',
                'product_based_organization',
                'cross_functional_responsibilities',
                'technology_silos_removed',
                'full_stack_ownership',
                'frontend_backend_merged',
                'devops_practices_adopted',
                'shared_team_ownership'
            ],
            'scenarios': ['13', '19'],
            'detection_queries': {
                'agile_transition': "team.methodology CHANGED TO 'agile'",
                'product_teams': "team.type = 'product' AND team.structure = 'cross-functional'"
            }
        },
        
        'compliance_specialization': {
            'indicators': [
                'compliance_role_added',
                'security_certifications_obtained',
                'legal_it_transfer',
                'audit_responsibilities_assigned',
                'regulatory_tool_access',
                'compliance_monitoring_setup',
                'policy_enforcement_activities',
                'risk_assessment_ownership'
            ],
            'scenarios': ['14'],
            'detection_queries': {
                'compliance_role': "role_additions INCLUDE 'compliance_officer'",
                'legal_transfer': "previous_owner.department = 'legal' AND new_owner.department = 'security'"
            }
        },
        
        'graduate_program_completion': {
            'indicators': [
                'rotation_program_ended',
                'permanent_assignment_made',
                'trainee_status_removed',
                'full_responsibilities_granted',
                'mentor_assignment_ended',
                'independent_project_ownership',
                'team_integration_complete',
                'specialization_area_chosen'
            ],
            'scenarios': ['16'],
            'detection_queries': {
                'graduate_completion': "user.program = 'graduate_trainee' AND program_end_date < NOW()",
                'permanent_role': "user_type CHANGED FROM 'trainee' TO 'permanent'"
            }
        },
        
        'founder_integration': {
            'indicators': [
                'founder_title_present',
                'executive_role_assigned',
                'startup_acquisition_complete',
                'corporate_integration_activities',
                'strategic_initiatives_led',
                'innovation_lab_creation',
                'new_technology_adoption',
                'cultural_transformation_activities'
            ],
            'scenarios': ['17'],
            'detection_queries': {
                'founder_pattern': "user.title LIKE '%founder%' OR user.title LIKE '%CEO%'",
                'acquisition': "company_acquired = true AND user.role = 'executive'"
            }
        },
        
        'regional_consolidation': {
            'indicators': [
                'multiple_regions_managed',
                'shared_service_center_created',
                'regional_boundaries_removed',
                'centralized_management_adopted',
                'standardization_activities',
                'cross_regional_access_granted',
                'unified_procedures_implemented',
                'regional_silos_eliminated'
            ],
            'scenarios': ['18'],
            'detection_queries': {
                'multi_region': "COUNT(DISTINCT location) > 1 GROUP BY user",
                'shared_service': "team.type = 'shared_service_center'"
            }
        },
        
        'devops_transformation': {
            'indicators': [
                'infrastructure_as_code_adopted',
                'automation_tools_implemented',
                'ci_cd_pipeline_ownership',
                'traditional_ops_deprecated',
                'developer_operations_merged',
                'deployment_automation_increased',
                'configuration_management_automated',
                'monitoring_integrated'
            ],
            'scenarios': ['19'],
            'detection_queries': {
                'devops_adoption': "tools INCLUDE ('terraform', 'ansible', 'jenkins')",
                'automation': "manual_changes DECREASED BY 80%"
            }
        },
        
        'remote_work_assignment': {
            'indicators': [
                'equipment_pool_checkout',
                'home_office_location_set',
                'remote_access_configured',
                'individual_device_assignment',
                'vpn_access_granted',
                'remote_support_enabled',
                'home_network_configured',
                'personal_admin_rights_granted'
            ],
            'scenarios': ['20'],
            'detection_queries': {
                'remote_assignment': "location CHANGED TO 'remote' OR location LIKE '%home%'",
                'equipment_checkout': "ci.previous_location = 'equipment_pool'"
            }
        },
        
        'emergency_response_formation': {
            'indicators': [
                'incident_response_team_created',
                'critical_infrastructure_assigned',
                '24x7_coverage_implemented',
                'emergency_procedures_activated',
                'rapid_response_capabilities',
                'security_clearance_elevated',
                'crisis_management_tools_access',
                'priority_escalation_rights'
            ],
            'scenarios': ['21'],
            'detection_queries': {
                'emergency_team': "team.type = 'emergency_response' AND priority = 'critical'",
                'always_on': "support_hours = '24x7'"
            }
        },
        
        'ml_specialization': {
            'indicators': [
                'gpu_infrastructure_assigned',
                'ml_frameworks_installed',
                'research_computing_transfer',
                'specialized_hardware_access',
                'data_science_tools_configured',
                'model_training_activities',
                'compute_cluster_management',
                'ai_platform_ownership'
            ],
            'scenarios': ['22'],
            'detection_queries': {
                'ml_infrastructure': "ci.type INCLUDE ('gpu', 'ml_server', 'compute_cluster')",
                'specialization': "user.skills ADDED 'machine_learning'"
            }
        },
        
        'qa_lead_promotion': {
            'indicators': [
                'qa_analyst_to_lead_promotion',
                'test_environment_inheritance',
                'team_supervision_added',
                'strategic_planning_responsibilities',
                'automation_framework_ownership',
                'quality_gate_management',
                'ci_cd_integration_ownership',
                'cross_team_coordination'
            ],
            'scenarios': ['23'],
            'detection_queries': {
                'qa_promotion': "title CHANGED FROM 'QA Analyst' TO 'QA Lead'",
                'leadership': "direct_reports > 0"
            }
        },
        
        'dr_specialization': {
            'indicators': [
                'disaster_recovery_focus',
                'backup_infrastructure_assigned',
                'dr_site_management',
                'recovery_testing_ownership',
                'backup_optimization_activities',
                'rto_rpo_management',
                'dr_documentation_ownership',
                'failover_procedure_control'
            ],
            'scenarios': ['24'],
            'detection_queries': {
                'dr_assignment': "ci.type INCLUDE ('backup', 'dr', 'recovery')",
                'specialization': "user.certifications ADDED 'disaster_recovery'"
            }
        },
        
        'customer_platform_migration': {
            'indicators': [
                'customer_facing_systems_assigned',
                'platform_migration_leadership',
                'customer_success_integration',
                'web_team_handover',
                'customer_portal_ownership',
                'experience_optimization_focus',
                'customer_data_management',
                'success_metrics_ownership'
            ],
            'scenarios': ['25'],
            'detection_queries': {
                'customer_platform': "ci.purpose INCLUDE 'customer' AND platform_migration = true",
                'team_change': "team CHANGED FROM 'web_development' TO 'customer_success'"
            }
        }
    }


    def predict_single(self, ci_data: Dict) -> Dict:
        """
        Predict staleness for a single CI
        Input format expected from ServiceNow data
        """
        try:
            # Extract features from ServiceNow data
            features = self._extract_features_from_servicenow_data(ci_data)
            
            # Apply rules
            triggered_rules = []
            total_confidence = 0

            for rule_name, rule_def in self.rules.items():
                if self._evaluate_rule(features, rule_def['conditions']):
                    triggered_rules.append({
                        'rule': rule_name,
                        'description': rule_def['description'],
                        'confidence': rule_def['confidence'],
                        'scenarios': rule_def['scenarios']
                    })
                    total_confidence = max(total_confidence, rule_def['confidence'])

            # Determine staleness
            is_stale = total_confidence > 0.7

            # Get recommendation for new owner
            new_owner_recommendation = self._recommend_new_owner_from_data(ci_data)

            return {
                'is_stale': is_stale,
                'confidence': total_confidence,
                'triggered_rules': triggered_rules,
                'new_owner_recommendation': new_owner_recommendation,
                'features': features
            }

        except Exception as e:
            return {
                'is_stale': False,
                'confidence': 0.0,
                'triggered_rules': [],
                'new_owner_recommendation': None,
                'error': str(e)
            }

    def _extract_features_from_servicenow_data(self, ci_data: Dict) -> Dict:
        """
        Extract features from ServiceNow data format
        Expected input structure:
        {
            'ci_info': {...},
            'audit_records': [...],
            'user_info': {...},
            'assigned_owner': '...'
        }
        """
        features = {}
        
        # Basic info
        assigned_owner = ci_data.get('assigned_owner', '')
        features['owner_name'] = assigned_owner
        
        # Audit records analysis
        audit_records = ci_data.get('audit_records', [])
        features['total_activity_count'] = len(audit_records)
        
        # Owner activity analysis
        owner_activities = [r for r in audit_records if r.get('user') == assigned_owner]
        features['owner_activity_count'] = len(owner_activities)
        
        if len(audit_records) > 0:
            features['owner_activity_ratio'] = len(owner_activities) / len(audit_records)
        else:
            features['owner_activity_ratio'] = 0

        # Days since owner activity
        if owner_activities:
            try:
                last_activity = max([self._parse_date(r.get('sys_created_on', '')) for r in owner_activities])
                features['days_since_owner_activity'] = (datetime.now() - last_activity).days
            except:
                features['days_since_owner_activity'] = 999
        else:
            features['days_since_owner_activity'] = 999

        # Other users analysis
        other_users = {}
        for record in audit_records:
            user = record.get('user', '')
            if user != assigned_owner and user:
                other_users[user] = other_users.get(user, 0) + 1

        features['other_users_count'] = len(other_users)
        
        if other_users:
            top_user = max(other_users.items(), key=lambda x: x[1])
            features['top_other_user'] = top_user[0]
            features['top_other_user_count'] = top_user[1]
            features['top_other_user_ratio'] = top_user[1] / len(audit_records)
        else:
            features['top_other_user'] = None
            features['top_other_user_count'] = 0
            features['top_other_user_ratio'] = 0

        # Recent activity (last 30 days)
        recent_cutoff = datetime.now() - timedelta(days=30)
        recent_other_activities = 0
        for record in audit_records:
            if record.get('user') != assigned_owner:
                try:
                    record_date = self._parse_date(record.get('sys_created_on', ''))
                    if record_date > recent_cutoff:
                        recent_other_activities += 1
                except:
                    pass
        features['recent_other_activities'] = recent_other_activities

        # User info analysis
        user_info = ci_data.get('user_info', {})
        features['owner_active'] = user_info.get('active', True)

        # Role and department changes (simplified)
        role_change_fields = ['title', 'role', 'department']
        role_changes = sum(1 for r in audit_records 
                          if r.get('fieldname') in role_change_fields and r.get('user') == assigned_owner)
        features['owner_role_changes'] = role_changes
        features['owner_title_changed'] = role_changes > 0
        features['owner_dept_changed'] = any(r.get('fieldname') == 'department' for r in audit_records)

        # Group status (simplified)
        features['assigned_group_active'] = True  # Default assumption

        # Ownership field changes by non-owner
        ownership_fields = ['assigned_to', 'managed_by', 'support_group']
        ownership_changes = sum(1 for r in audit_records 
                              if r.get('fieldname') in ownership_fields and r.get('user') != assigned_owner)
        features['non_owner_ownership_changes'] = ownership_changes

        return features

    def _parse_date(self, date_str: str) -> datetime:
        """Parse ServiceNow date format"""
        try:
            # Try common ServiceNow date formats
            formats = [
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
            
            # If all formats fail, return a very old date
            return datetime(1900, 1, 1)
        except:
            return datetime(1900, 1, 1)

    def _clean_department_field(self, dept_field):
        """Clean department field to remove ServiceNow link information"""
        if not dept_field:
            return 'Unknown'
        
        # If it's a simple string, return as is
        if isinstance(dept_field, str) and not dept_field.startswith('{') and 'link' not in dept_field.lower():
            return dept_field
        
        # If it's a dict (ServiceNow reference field), extract display_value
        if isinstance(dept_field, dict):
            return dept_field.get('display_value', dept_field.get('value', 'Unknown'))
        
        # If it's a string that looks like a dict/JSON or contains link info, clean it up
        if isinstance(dept_field, str) and (dept_field.startswith('{') or 'link' in dept_field.lower()):
            try:
                import json
                if dept_field.startswith('{'):
                    dept_data = json.loads(dept_field)
                    return dept_data.get('display_value', dept_data.get('value', 'Unknown'))
                else:
                    # If it contains link info but isn't JSON, just return "Unknown"
                    return 'Unknown'
            except:
                return 'Unknown'
        
        # Default fallback
        return str(dept_field) if dept_field else 'Unknown'

    def _evaluate_rule(self, features: Dict, conditions: List[str]) -> bool:
        """Evaluate if all conditions are met"""
        for condition in conditions:
            try:
                # Create local variables for evaluation
                local_vars = features.copy()
                
                # Evaluate condition
                if not eval(condition, {"__builtins__": {}}, local_vars):
                    return False
            except:
                return False
        return True

    def _recommend_new_owner_from_data(self, ci_data: Dict) -> Optional[Dict]:
        """Recommend new owner based on ServiceNow data"""
        try:
            audit_records = ci_data.get('audit_records', [])
            assigned_owner = ci_data.get('assigned_owner', '')
            username_to_display_name = ci_data.get('username_to_display_name', {})
            
            # Get user data for department lookup
            user_info_lookup = {}
            if 'user_info' in ci_data:
                # This is the user info for the assigned owner, but we need all users
                pass
            
            # Build user lookup from audit records and try to match with user data
            # We'll need to get user data from the broader context
            
            if not audit_records:
                return None

            # Analyze user activities
            user_activities = {}
            for record in audit_records:
                user = record.get('user', '')
                if user and user != assigned_owner:
                    if user not in user_activities:
                        user_activities[user] = {
                            'count': 0,
                            'last_activity': None,
                            'first_activity': None,
                            'fields': set()
                        }
                    
                    user_activities[user]['count'] += 1
                    user_activities[user]['fields'].add(record.get('fieldname', ''))
                    
                    try:
                        activity_date = self._parse_date(record.get('sys_created_on', ''))
                        if user_activities[user]['last_activity'] is None or activity_date > user_activities[user]['last_activity']:
                            user_activities[user]['last_activity'] = activity_date
                        if user_activities[user]['first_activity'] is None or activity_date < user_activities[user]['first_activity']:
                            user_activities[user]['first_activity'] = activity_date
                    except:
                        pass

            if not user_activities:
                return None

            # Calculate scores for all users
            user_scores = []

            for user, data in user_activities.items():
                score = 0
                
                # Activity volume (40%)
                score += (data['count'] / len(audit_records)) * 40
                
                # Recency (30%)
                if data['last_activity']:
                    days_since = (datetime.now() - data['last_activity']).days
                    recency_score = max(0, 100 - days_since) / 100
                    score += recency_score * 30
                
                # Ownership field modifications (20%)
                ownership_fields = {'assigned_to', 'managed_by'}
                ownership_mods = len(data['fields'].intersection(ownership_fields))
                score += (ownership_mods > 0) * 20
                
                # Consistency (10%)
                if data['count'] > 1 and data['first_activity'] and data['last_activity']:
                    activity_span = (data['last_activity'] - data['first_activity']).days
                    if activity_span > 0:
                        consistency = data['count'] / activity_span
                        score += min(consistency * 10, 10)

                # Get display name for this user - handle both sys_id and username cases
                display_name = user
                department = 'Unknown'
                
                # Try multiple strategies to get the display name
                user_data_context = ci_data.get('user_data_context', {})
                user_by_sys_id = ci_data.get('user_by_sys_id', {})
                
                # Strategy 1: Check if user is in username_to_display_name mapping (user is username)
                if user in username_to_display_name:
                    display_name = username_to_display_name[user]
                    if user in user_data_context:
                        dept_raw = user_data_context[user].get('department', 'Unknown')
                        department = self._clean_department_field(dept_raw)
                
                # Strategy 2: Check if user is a sys_id in user_by_sys_id mapping
                elif user in user_by_sys_id:
                    user_record = user_by_sys_id[user]
                    display_name = user_record.get('name', user)
                    dept_raw = user_record.get('department', 'Unknown')
                    department = self._clean_department_field(dept_raw)
                
                # Strategy 3: Try to get display name from audit records if available
                else:
                    for record in audit_records:
                        if record.get('user') == user and record.get('user_display_name'):
                            display_name = record.get('user_display_name')
                            break
                
                # Strategy 4: If user looks like a sys_id (long hex string), try to find it in user data
                if display_name == user and len(user) > 20 and all(c in '0123456789abcdef' for c in user.lower()):
                    # This looks like a sys_id, try to find the corresponding user
                    for username, user_record in user_data_context.items():
                        if user_record.get('sys_id') == user:
                            display_name = user_record.get('name', username)
                            dept_raw = user_record.get('department', 'Unknown')
                            department = self._clean_department_field(dept_raw)
                            break

                # Determine the actual username to use for assignment
                actual_username = user
                
                # If user looks like a sys_id, find the corresponding username
                if len(user) > 20 and all(c in '0123456789abcdef' for c in user.lower()):
                    # This looks like a sys_id, try to find the corresponding username
                    for username, user_record in user_data_context.items():
                        if user_record.get('sys_id') == user:
                            actual_username = username
                            break
                    # Also check user_by_sys_id mapping
                    if actual_username == user and user in user_by_sys_id:
                        user_record = user_by_sys_id[user]
                        actual_username = user_record.get('user_name', user)

                user_scores.append({
                    'user': actual_username,  # Use actual username for assignment
                    'display_name': display_name,
                    'score': score,
                    'activity_count': data['count'],
                    'last_activity_days_ago': (datetime.now() - data['last_activity']).days if data['last_activity'] else 999,
                    'fields_modified': len(data['fields']),
                    'ownership_changes': len(data['fields'].intersection(ownership_fields)),
                    'department': department
                })

            # Sort by score descending and return top recommendations
            user_scores.sort(key=lambda x: x['score'], reverse=True)
            
            # Return top 5 recommendations (or all if less than 5)
            return user_scores[:5] if user_scores else None

        except Exception as e:
            return None

    def get_stale_ci_list(self, labels_df, audit_df, user_df, ci_df, ci_owner_display_names=None):
        """
        Analyze all CIs and return a list of stale CIs with confidence and risk level.
        Args:
            labels_df: DataFrame with columns ['ci_id', 'assigned_owner']
            audit_df: DataFrame of audit records
            user_df: DataFrame of user records
            ci_df: DataFrame of CI records
            ci_owner_display_names: Dict mapping CI IDs to owner display names
        Returns:
            List of dicts, each representing a stale CI with confidence and risk_level
        """
        stale_cis = []
        if ci_owner_display_names is None:
            ci_owner_display_names = {}
            
        # Convert user and CI data to regular dicts first
        user_by_name = {}
        user_by_sys_id = {}
        username_to_display_name = {}  # Create mapping for display names
        for _, u in user_df.iterrows():
            user_dict = {k: v for k, v in u.to_dict().items()}
            if u.get('user_name'):
                user_name = str(u.get('user_name'))
                user_by_name[user_name] = user_dict
                # Map username to display name
                display_name = str(u.get('name', user_name))  # Use 'name' field as display name, fallback to username
                username_to_display_name[user_name] = display_name
            if u.get('sys_id'):
                sys_id = str(u.get('sys_id'))
                user_by_sys_id[sys_id] = user_dict
        
        # Build lookup for audit data
        audit_by_ci = {}
        for _, row in audit_df.iterrows():
            doc_key = row.get('documentkey')
            
            # Extract the actual document key value if it's a dict
            if isinstance(doc_key, dict):
                doc_key = doc_key.get('value', doc_key.get('display_value', ''))
            
            if doc_key:
                # Convert pandas Series to dict to avoid JSON serialization issues
                audit_record = {k: v for k, v in row.to_dict().items()}
                
                # Clean up all dict fields to extract their values
                for field_name, field_value in audit_record.items():
                    if isinstance(field_value, dict):
                        # For most fields, prefer display_value, fallback to value
                        if field_name == 'sys_created_on':
                            # For dates, prefer the actual datetime value
                            audit_record[field_name] = field_value.get('value', field_value.get('display_value', ''))
                        else:
                            audit_record[field_name] = field_value.get('display_value', field_value.get('value', ''))
                
                # Extract user display name from expanded user fields if available
                user_field = row.get('user')  # Get original user field from row
                if isinstance(user_field, dict):
                    # If user is expanded, extract the display name
                    user_display_name = user_field.get('display_value', '')
                    user_name_field = row.get('user.user_name')
                    if isinstance(user_name_field, dict):
                        user_username = user_name_field.get('display_value', user_name_field.get('value', ''))
                    else:
                        user_username = user_name_field or user_field.get('value', '')
                    
                    # Update the audit record with clean user information
                    audit_record['user'] = user_username
                    audit_record['user_display_name'] = user_display_name
                elif user_field:
                    # If user is just a string, try to get display name from user data
                    audit_record['user'] = str(user_field)
                    audit_record['user_display_name'] = username_to_display_name.get(str(user_field), str(user_field))
                
                audit_by_ci.setdefault(str(doc_key), []).append(audit_record)
        
        ci_by_id = {}
        for _, ci in ci_df.iterrows():
            ci_sys_id = ci.get('sys_id')
            # Handle sys_id that might be a dict with display_value/value
            if isinstance(ci_sys_id, dict):
                ci_sys_id = ci_sys_id.get('value', ci_sys_id.get('display_value', ''))
            
            if ci_sys_id:
                ci_by_id[str(ci_sys_id)] = {k: v for k, v in ci.to_dict().items()}
        
        print(f"DEBUG: Built ci_by_id mapping with {len(ci_by_id)} entries")
        if ci_by_id:
            sample_ci_id = list(ci_by_id.keys())[0]
            sample_ci_data = ci_by_id[sample_ci_id]
            print(f"DEBUG: Sample CI {sample_ci_id} has keys: {list(sample_ci_data.keys())}")
            if 'name' in sample_ci_data:
                print(f"DEBUG: Sample CI name: {sample_ci_data['name']} (type: {type(sample_ci_data['name'])})")

        for _, label in labels_df.iterrows():
            # Convert label to dict to avoid pandas Series issues
            label_dict = label.to_dict()
            ci_id = label_dict.get('ci_id')
            assigned_owner = label_dict.get('assigned_owner')
            
            ci_info = ci_by_id.get(str(ci_id), {})
            audit_records = audit_by_ci.get(str(ci_id), [])
            user_info = user_by_name.get(str(assigned_owner), {})
            
            # Debug logging for first few CIs to see CI info lookup
            if len(stale_cis) < 3:
                print(f"DEBUG CI Lookup {ci_id}: ci_info keys={list(ci_info.keys()) if ci_info else 'EMPTY'}")
                if ci_info and 'name' in ci_info:
                    print(f"DEBUG CI name field: {ci_info['name']} (type: {type(ci_info['name'])})")
            
            ci_data = {
                'ci_info': ci_info,
                'audit_records': audit_records,
                'user_info': user_info,
                'assigned_owner': assigned_owner,
                'username_to_display_name': username_to_display_name,  # Pass the mapping
                'user_data_context': user_by_name,  # Pass all user data for department lookup
                'user_by_sys_id': user_by_sys_id  # Pass sys_id mapping for better lookups
            }
            
            result = self.predict_single(ci_data)
            if result.get('is_stale'):
                # Assign risk level based on confidence
                confidence = result.get('confidence', 0)
                if confidence > 0.9:
                    risk_level = 'Critical'
                elif confidence > 0.8:
                    risk_level = 'High'
                elif confidence > 0.7:
                    risk_level = 'Medium'
                else:
                    risk_level = 'Low'
                
                # Ensure all data is JSON serializable
                # Get the display name for the current owner - try CI mapping first, then username mapping
                ci_mapping_result = ci_owner_display_names.get(str(ci_id))
                username_mapping_result = username_to_display_name.get(str(assigned_owner))
                current_owner_display_name = ci_mapping_result or username_mapping_result or str(assigned_owner)
                
                # Debug logging for first few CIs
                if len(stale_cis) < 3:
                    print(f"DEBUG CI {ci_id}: assigned_owner='{assigned_owner}', ci_mapping='{ci_mapping_result}', username_mapping='{username_mapping_result}', final='{current_owner_display_name}'")
                
                # Extract CI name properly (might be a dict with display_value/value)
                ci_name = ci_info.get('name', 'Unknown')
                if isinstance(ci_name, dict):
                    ci_name = ci_name.get('display_value', ci_name.get('value', 'Unknown'))
                
                # Extract CI class properly
                ci_class = ci_info.get('sys_class_name', 'Unknown')
                if isinstance(ci_class, dict):
                    ci_class = ci_class.get('display_value', ci_class.get('value', 'Unknown'))
                
                # Extract CI description properly
                ci_description = ci_info.get('short_description', '')
                if isinstance(ci_description, dict):
                    ci_description = ci_description.get('display_value', ci_description.get('value', ''))
                
                stale_ci_dict = {
                    'ci_id': str(ci_id),
                    'ci_name': str(ci_name),
                    'ci_class': str(ci_class),
                    'ci_description': str(ci_description),
                    'current_owner': current_owner_display_name,
                    'current_owner_username': str(assigned_owner),  # Keep username for technical reference
                    'confidence': float(confidence),
                    'risk_level': str(risk_level),
                    'staleness_reasons': [
                        {
                            'rule_name': str(rule.get('rule', '')),
                            'description': str(rule.get('description', '')),
                            'confidence': float(rule.get('confidence', 0))
                        } for rule in result.get('triggered_rules', [])
                    ],
                    'recommended_owners': self._format_owner_recommendations(result.get('new_owner_recommendation')),
                    'owner_activity_count': int(result.get('features', {}).get('owner_activity_count', 0)),
                    'days_since_owner_activity': int(result.get('features', {}).get('days_since_owner_activity', 999)),
                    'owner_active': bool(result.get('features', {}).get('owner_active', True))
                }

                stale_cis.append(stale_ci_dict)

        return stale_cis

    def _format_owner_recommendations(self, recommendations):
        """Format owner recommendations to be JSON serializable"""
        if not recommendations:
            return []
        
        # Handle both single recommendation (old format) and multiple recommendations (new format)
        if isinstance(recommendations, dict):
            # Old format - single recommendation
            return [{
                'username': str(recommendations.get('user', '')),
                'display_name': str(recommendations.get('display_name', recommendations.get('user', ''))),
                'score': int(recommendations.get('score', 0)),
                'activity_count': int(recommendations.get('activity_count', 0)),
                'last_activity_days_ago': int(recommendations.get('last_activity_days_ago', 999)),
                'ownership_changes': int(recommendations.get('ownership_changes', 0)),
                'fields_modified': int(recommendations.get('fields_modified', 0)),
                'department': str(recommendations.get('department', 'Unknown'))
            }]
        elif isinstance(recommendations, list):
            # New format - multiple recommendations
            formatted_recommendations = []
            for rec in recommendations:
                formatted_recommendations.append({
                    'username': str(rec.get('user', '')),
                    'display_name': str(rec.get('display_name', rec.get('user', ''))),
                    'score': int(rec.get('score', 0)),
                    'activity_count': int(rec.get('activity_count', 0)),
                    'last_activity_days_ago': int(rec.get('last_activity_days_ago', 999)),
                    'ownership_changes': int(rec.get('ownership_changes', 0)),
                    'fields_modified': int(rec.get('fields_modified', 0)),
                    'department': str(rec.get('department', 'Unknown'))
                })
            return formatted_recommendations
        
        return []


# Create and save the model
def create_and_save_model():
    """Create the model and save it as pickle file"""
    print("Creating Rule-Based Staleness Detector...")
    
    # Initialize the detector
    detector = RuleBasedStalenessDetector()
    
    # Save as pickle file
    model_filename = 'staleness_detector_model.pkl'
    
    with open(model_filename, 'wb') as f:
        pickle.dump(detector, f)
    
    print(f"Model saved as {model_filename}")
    
    # Test loading the model
    print("Testing model loading...")
    with open(model_filename, 'rb') as f:
        loaded_model = pickle.load(f)
    
    # Test prediction with sample data
    sample_ci_data = {
        'assigned_owner': 'john.doe',
        'audit_records': [
            {
                'user': 'jane.smith',
                'sys_created_on': '2024-06-20 10:30:00',
                'fieldname': 'assigned_to'
            },
            {
                'user': 'jane.smith',
                'sys_created_on': '2024-06-21 14:15:00',
                'fieldname': 'state'
            }
        ],
        'user_info': {
            'active': True
        }
    }
    
    result = loaded_model.predict_single(sample_ci_data)
    print("Test prediction successful!")
    print(f"Sample result: {result}")
    
    return model_filename

if __name__ == "__main__":
    model_file = create_and_save_model()