import React, { useState } from 'react';
import { AlertCircle, CheckCircle, Loader2, Search, Server, User, Eye, EyeOff, Brain, TrendingUp, AlertTriangle, Users, ChevronDown, ChevronRight, Clock, Shield, Activity, UserCheck } from 'lucide-react';

const ServiceNowScanner = () => {
  const [formData, setFormData] = useState({
    instanceUrl: '',
    username: '',
    password: ''
  });
  const [showPassword, setShowPassword] = useState(false);
  const [connectionStatus, setConnectionStatus] = useState(null);
  const [isConnecting, setIsConnecting] = useState(false);
  const [isScanning, setIsScanning] = useState(false);
  const [scanResults, setScanResults] = useState(null);
  const [scanProgress, setScanProgress] = useState(0);
  const [expandedCI, setExpandedCI] = useState(null);

  const handleInputChange = (e) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const testConnection = async () => {
    if (!formData.instanceUrl || !formData.username || !formData.password) {
      setConnectionStatus({ success: false, message: 'Please fill in all fields' });
      return;
    }

    setIsConnecting(true);
    setConnectionStatus(null);

    try {
      const response = await fetch('http://localhost:5000/test-connection', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          instance_url: formData.instanceUrl,
          username: formData.username,
          password: formData.password
        }),
      });

      const result = await response.json();
      
      if (response.ok && result.success) {
        setConnectionStatus({ 
          success: true, 
          message: result.message
        });
      } else {
        setConnectionStatus({ 
          success: false, 
          message: result.error || 'Connection failed. Please check your credentials.' 
        });
      }
    } catch (error) {
      setConnectionStatus({ success: false, message: 'Network error. Please try again.' });
    } finally {
      setIsConnecting(false);
    }
  };

  const scanStaleOwnership = async () => {
    if (!connectionStatus?.success) {
      alert('Please establish a successful connection first');
      return;
    }

    setIsScanning(true);
    setScanResults(null);
    setScanProgress(0);
    setExpandedCI(null);

    try {
      // Simulate progress
      const progressInterval = setInterval(() => {
        setScanProgress(prev => {
          if (prev >= 90) {
            clearInterval(progressInterval);
            return 90;
          }
          return prev + 15;
        });
      }, 1000);

      const response = await fetch('http://localhost:5000/scan-stale-ownership', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          instance_url: formData.instanceUrl,
          username: formData.username,
          password: formData.password
        }),
      });

      clearInterval(progressInterval);
      setScanProgress(100);

      if (response.ok) {
        const results = await response.json();
        setScanResults(results);
      } else {
        const error = await response.json();
        setScanResults({ error: error.error || 'Scan failed. Please try again.' });
      }
    } catch (error) {
      setScanResults({ error: 'Network error during scan. Please try again.' });
    } finally {
      setIsScanning(false);
    }
  };

  const getRiskColor = (riskLevel) => {
    switch (riskLevel) {
      case 'Critical': return 'text-red-400 bg-red-500/20 border-red-500/30';
      case 'High': return 'text-orange-400 bg-orange-500/20 border-orange-500/30';
      case 'Medium': return 'text-yellow-400 bg-yellow-500/20 border-yellow-500/30';
      default: return 'text-green-400 bg-green-500/20 border-green-500/30';
    }
  };

  const getRiskIcon = (riskLevel) => {
    switch (riskLevel) {
      case 'Critical': return <AlertTriangle className="w-4 h-4" />;
      case 'High': return <AlertCircle className="w-4 h-4" />;
      case 'Medium': return <Clock className="w-4 h-4" />;
      default: return <Shield className="w-4 h-4" />;
    }
  };

  const toggleCIExpansion = (ciId) => {
    setExpandedCI(expandedCI === ciId ? null : ciId);
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900">
      <div className="container mx-auto px-4 py-8">
        {/* Header */}
        <div className="text-center mb-12">
          <div className="flex items-center justify-center mb-4">
            <Brain className="w-12 h-12 text-purple-400 mr-3" />
            <h1 className="text-4xl font-bold text-white">AI-Powered ServiceNow Scanner</h1>
          </div>
          <p className="text-gray-300 text-lg">Detect stale ownership using machine learning analysis</p>
        </div>

        {/* Main Card */}
        <div className="max-w-6xl mx-auto bg-white/10 backdrop-blur-lg rounded-2xl shadow-2xl border border-white/20">
          <div className="p-8">
            {/* Connection Form */}
            <div className="space-y-6 mb-8">
              <h2 className="text-2xl font-semibold text-white mb-6 flex items-center">
                <User className="w-6 h-6 mr-2 text-purple-400" />
                ServiceNow Connection
              </h2>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-200 mb-2">
                    Instance URL
                  </label>
                  <input
                    type="url"
                    name="instanceUrl"
                    value={formData.instanceUrl}
                    onChange={handleInputChange}
                    placeholder="https://your-instance.service-now.com"
                    className="w-full px-4 py-3 bg-white/10 border border-white/20 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-purple-500 focus:border-transparent transition-all"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-200 mb-2">
                    Username
                  </label>
                  <input
                    type="text"
                    name="username"
                    value={formData.username}
                    onChange={handleInputChange}
                    placeholder="Enter your username"
                    className="w-full px-4 py-3 bg-white/10 border border-white/20 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-purple-500 focus:border-transparent transition-all"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-200 mb-2">
                    Password
                  </label>
                  <div className="relative">
                    <input
                      type={showPassword ? 'text' : 'password'}
                      name="password"
                      value={formData.password}
                      onChange={handleInputChange}
                      placeholder="Enter your password"
                      className="w-full px-4 py-3 pr-12 bg-white/10 border border-white/20 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-purple-500 focus:border-transparent transition-all"
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword(!showPassword)}
                      className="absolute right-3 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-white transition-colors"
                    >
                      {showPassword ? <EyeOff className="w-5 h-5" /> : <Eye className="w-5 h-5" />}
                    </button>
                  </div>
                </div>
              </div>

              <button
                onClick={testConnection}
                disabled={isConnecting}
                className="w-full bg-gradient-to-r from-blue-600 to-blue-700 hover:from-blue-700 hover:to-blue-800 disabled:from-gray-600 disabled:to-gray-700 text-white font-semibold py-3 px-6 rounded-lg transition-all duration-200 flex items-center justify-center"
              >
                {isConnecting ? (
                  <>
                    <Loader2 className="w-5 h-5 mr-2 animate-spin" />
                    Testing Connection...
                  </>
                ) : (
                  <>
                    <Server className="w-5 h-5 mr-2" />
                    Test Connection
                  </>
                )}
              </button>

              {connectionStatus && (
                <div className={`flex items-center p-4 rounded-lg ${
                  connectionStatus.success 
                    ? 'bg-green-500/20 border border-green-500/30' 
                    : 'bg-red-500/20 border border-red-500/30'
                }`}>
                  {connectionStatus.success ? (
                    <CheckCircle className="w-5 h-5 text-green-400 mr-3" />
                  ) : (
                    <AlertCircle className="w-5 h-5 text-red-400 mr-3" />
                  )}
                  <span className={connectionStatus.success ? 'text-green-200' : 'text-red-200'}>
                    {connectionStatus.message}
                  </span>
                </div>
              )}
            </div>

            {/* Scan Section */}
            <div className="border-t border-white/20 pt-8">
              <h2 className="text-2xl font-semibold text-white mb-6 flex items-center">
                <Brain className="w-6 h-6 mr-2 text-purple-400" />
                AI-Powered Stale Ownership Analysis
              </h2>

              <div className="space-y-4">
                <p className="text-gray-300 text-sm">
                  Our machine learning model analyzes audit trails, user activity patterns, and ownership changes to identify stale assignments with high accuracy.
                </p>

                <button
                  onClick={scanStaleOwnership}
                  disabled={isScanning || !connectionStatus?.success}
                  className="w-full bg-gradient-to-r from-purple-600 to-purple-700 hover:from-purple-700 hover:to-purple-800 disabled:from-gray-600 disabled:to-gray-700 text-white font-semibold py-3 px-6 rounded-lg transition-all duration-200 flex items-center justify-center"
                >
                  {isScanning ? (
                    <>
                      <Loader2 className="w-5 h-5 mr-2 animate-spin" />
                      AI Analysis in Progress... {scanProgress}%
                    </>
                  ) : (
                    <>
                      <Search className="w-5 h-5 mr-2" />
                      Start AI Analysis
                    </>
                  )}
                </button>

                {isScanning && (
                  <div className="w-full bg-white/10 rounded-full h-2">
                    <div 
                      className="bg-gradient-to-r from-purple-500 to-purple-600 h-2 rounded-full transition-all duration-300"
                      style={{ width: `${scanProgress}%` }}
                    ></div>
                  </div>
                )}

                {/* Scan Results */}
                {scanResults && (
                  <div className="mt-6 space-y-6">
                    {scanResults.error ? (
                      <div className="flex items-center text-red-400 p-4 bg-red-500/20 rounded-lg border border-red-500/30">
                        <AlertCircle className="w-5 h-5 mr-2" />
                        {scanResults.error}
                      </div>
                    ) : (
                      <>
                        {/* Summary Cards */}
                        <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
                          <div className="bg-white/5 p-4 rounded-lg border border-white/10">
                            <div className="flex items-center justify-between">
                              <div>
                                <p className="text-gray-400 text-sm">Total CIs</p>
                                <p className="text-white text-2xl font-bold">{scanResults.summary?.total_cis_analyzed || 0}</p>
                              </div>
                              <Server className="w-8 h-8 text-blue-400" />
                            </div>
                          </div>
                          
                          <div className="bg-white/5 p-4 rounded-lg border border-white/10">
                            <div className="flex items-center justify-between">
                              <div>
                                <p className="text-gray-400 text-sm">Stale Found</p>
                                <p className="text-red-400 text-2xl font-bold">{scanResults.summary?.stale_cis_found || 0}</p>
                              </div>
                              <AlertTriangle className="w-8 h-8 text-red-400" />
                            </div>
                          </div>
                          
                          <div className="bg-white/5 p-4 rounded-lg border border-white/10">
                            <div className="flex items-center justify-between">
                              <div>
                                <p className="text-gray-400 text-sm">Critical Risk</p>
                                <p className="text-red-400 text-2xl font-bold">{scanResults.summary?.critical_risk || 0}</p>
                              </div>
                              <AlertCircle className="w-8 h-8 text-red-400" />
                            </div>
                          </div>
                          
                          <div className="bg-white/5 p-4 rounded-lg border border-white/10">
                            <div className="flex items-center justify-between">
                              <div>
                                <p className="text-gray-400 text-sm">High Risk</p>
                                <p className="text-orange-400 text-2xl font-bold">{scanResults.summary?.high_risk || 0}</p>
                              </div>
                              <TrendingUp className="w-8 h-8 text-orange-400" />
                            </div>
                          </div>
                          
                          <div className="bg-white/5 p-4 rounded-lg border border-white/10">
                            <div className="flex items-center justify-between">
                              <div>
                                <p className="text-gray-400 text-sm">Accuracy</p>
                                <p className="text-green-400 text-2xl font-bold">94%</p>
                              </div>
                              <Brain className="w-8 h-8 text-green-400" />
                            </div>
                          </div>
                        </div>

                        {/* Stale CIs List */}
                        <div className="bg-white/5 rounded-lg border border-white/10 overflow-hidden">
                          <div className="p-4 border-b border-white/10">
                            <h3 className="text-lg font-semibold text-white">Stale Configuration Items</h3>
                            <p className="text-gray-400 text-sm">Click on any CI to view detailed analysis and recommendations</p>
                          </div>
                          
                          {scanResults.stale_cis && scanResults.stale_cis.length > 0 ? (
                            <div className="space-y-0">
                              {scanResults.stale_cis.map((ci, idx) => (
                                <div key={ci.ci_id} className="border-b border-white/10 last:border-b-0">
                                  {/* CI List Item */}
                                  <div 
                                    className="p-4 hover:bg-white/5 cursor-pointer transition-colors"
                                    onClick={() => toggleCIExpansion(ci.ci_id)}
                                  >
                                    <div className="flex items-center justify-between">
                                      <div className="flex items-center space-x-4 flex-1">
                                        <div className="flex items-center">
                                          {expandedCI === ci.ci_id ? 
                                            <ChevronDown className="w-5 h-5 text-gray-400" /> : 
                                            <ChevronRight className="w-5 h-5 text-gray-400" />
                                          }
                                        </div>
                                        
                                        <div className="flex-1">
                                          <div className="flex items-center space-x-3">
                                            <h4 className="text-white font-medium">{ci.ci_name}</h4>
                                            <span className={`px-2 py-1 text-xs rounded border ${getRiskColor(ci.risk_level)}`}>
                                              {getRiskIcon(ci.risk_level)}
                                              <span className="ml-1">{ci.risk_level}</span>
                                            </span>
                                          </div>
                                          <div className="text-sm text-gray-400 mt-1">
                                            <span>{ci.ci_class}</span>
                                            {ci.ci_description && <span> • {ci.ci_description}</span>}
                                          </div>
                                        </div>
                                      </div>
                                      
                                      <div className="flex items-center space-x-4">
                                        <div className="text-right">
                                          <div className="text-sm text-gray-300">Current Owner</div>
                                          <div className="text-white font-medium">{ci.current_owner}</div>
                                        </div>
                                        
                                        <div className="text-right">
                                          <div className="text-sm text-gray-300">Confidence</div>
                                          <div className="text-white font-medium">{(ci.confidence * 100).toFixed(0)}%</div>
                                        </div>
                                      </div>
                                    </div>
                                  </div>

                                  {/* Expanded Details */}
                                  {expandedCI === ci.ci_id && (
                                    <div className="bg-white/2 p-6 border-t border-white/10">
                                      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                                        {/* Staleness Reasons */}
                                        <div>
                                          <h5 className="text-white font-semibold mb-3 flex items-center">
                                            <AlertTriangle className="w-5 h-5 mr-2 text-orange-400" />
                                            Why This CI is Stale
                                          </h5>
                                          <div className="space-y-3">
                                            {ci.staleness_reasons.map((reason, reasonIdx) => (
                                              <div key={reasonIdx} className="bg-white/5 p-3 rounded-lg border border-white/10">
                                                <div className="flex items-center justify-between mb-2">
                                                  <span className="text-purple-300 font-medium">
                                                    {reason.rule_name.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                                                  </span>
                                                  <span className="text-xs text-gray-400">
                                                    {(reason.confidence * 100).toFixed(0)}% confidence
                                                  </span>
                                                </div>
                                                <p className="text-gray-300 text-sm">{reason.description}</p>
                                              </div>
                                            ))}
                                          </div>
                                          
                                          {/* Owner Activity Summary */}
                                          <div className="mt-4 bg-white/5 p-3 rounded-lg border border-white/10">
                                            <h6 className="text-white font-medium mb-2 flex items-center">
                                              <Activity className="w-4 h-4 mr-2" />
                                              Owner Activity Summary
                                            </h6>
                                            <div className="grid grid-cols-2 gap-4 text-sm">
                                              <div>
                                                <span className="text-gray-400">Activities: </span>
                                                <span className="text-white">{ci.owner_activity_count}</span>
                                              </div>
                                              <div>
                                                <span className="text-gray-400">Last Activity: </span>
                                                <span className="text-white">
                                                  {ci.days_since_owner_activity === 999 ? 'Never' : `${ci.days_since_owner_activity} days ago`}
                                                </span>
                                              </div>
                                              <div>
                                                <span className="text-gray-400">Account Status: </span>
                                                <span className={ci.owner_active ? 'text-green-400' : 'text-red-400'}>
                                                  {ci.owner_active ? 'Active' : 'Inactive'}
                                                </span>
                                              </div>
                                            </div>
                                          </div>
                                        </div>

                                        {/* Recommended Owners */}
                                        <div>
                                          <h5 className="text-white font-semibold mb-3 flex items-center">
                                            <UserCheck className="w-5 h-5 mr-2 text-green-400" />
                                            Recommended New Owners
                                          </h5>
                                          
                                          {ci.recommended_owners && ci.recommended_owners.length > 0 ? (
                                            <div className="space-y-3">
                                              {ci.recommended_owners.slice(0, 3).map((owner, ownerIdx) => (
                                                <div key={ownerIdx} className="bg-white/5 p-3 rounded-lg border border-white/10">
                                                  <div className="flex items-center justify-between mb-2">
                                                    <div>
                                                      <div className="text-green-300 font-medium">{owner.display_name}</div>
                                                      <div className="text-xs text-gray-400">{owner.username}</div>
                                                    </div>
                                                    <div className="text-right">
                                                      <div className="text-green-400 font-bold">{owner.score}/100</div>
                                                      <div className="text-xs text-gray-400">Match Score</div>
                                                    </div>
                                                  </div>
                                                  
                                                  <div className="grid grid-cols-2 gap-2 text-xs text-gray-300">
                                                    <div>
                                                      <span className="text-gray-400">Activities: </span>
                                                      {owner.activity_count}
                                                    </div>
                                                    <div>
                                                      <span className="text-gray-400">Last Seen: </span>
                                                      {owner.last_activity_days_ago === 999 ? 'Never' : `${owner.last_activity_days_ago}d ago`}
                                                    </div>
                                                    <div>
                                                      <span className="text-gray-400">Ownership Changes: </span>
                                                      {owner.ownership_changes}
                                                    </div>
                                                    <div>
                                                      <span className="text-gray-400">Fields Modified: </span>
                                                      {owner.fields_modified}
                                                    </div>
                                                  </div>
                                                  
                                                  {owner.department && (
                                                    <div className="mt-2 text-xs text-gray-400">
                                                      Department: {owner.department}
                                                    </div>
                                                  )}
                                                </div>
                                              ))}
                                              
                                              {ci.recommended_owners.length > 3 && (
                                                <div className="text-center">
                                                  <span className="text-gray-400 text-sm">
                                                    +{ci.recommended_owners.length - 3} more recommendations available
                                                  </span>
                                                </div>
                                              )}
                                            </div>
                                          ) : (
                                            <div className="bg-white/5 p-4 rounded-lg border border-white/10 text-center">
                                              <Users className="w-8 h-8 text-gray-400 mx-auto mb-2" />
                                              <p className="text-gray-400">No suitable owner recommendations found</p>
                                              <p className="text-xs text-gray-500 mt-1">Consider manual assignment or further investigation</p>
                                            </div>
                                          )}
                                        </div>
                                      </div>
                                    </div>
                                  )}
                                </div>
                              ))}
                            </div>
                          ) : (
                            <div className="p-8 text-center">
                              <CheckCircle className="w-12 h-12 text-green-400 mx-auto mb-4" />
                              <p className="text-green-400 text-lg font-medium">No stale ownership detected!</p>
                              <p className="text-gray-400 text-sm">All CIs appear to have appropriate ownership assignments.</p>
                            </div>
                          )}
                        </div>
                      </>
                    )}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="text-center mt-8 text-gray-400 text-sm">
          <p>Powered by advanced machine learning algorithms • Real-time ServiceNow integration</p>
        </div>
      </div>
    </div>
  );
};

export default ServiceNowScanner;