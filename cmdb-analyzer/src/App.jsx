import React, { useState } from 'react';
import { AlertCircle, CheckCircle, Loader2, Search, Server, User, Lock, Eye, EyeOff } from 'lucide-react';
import './index.css'

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
      // Make API call to Flask backend to test ServiceNow connection
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
          message: result.message,
          details: result.details 
        });
      } else {
        setConnectionStatus({ 
          success: false, 
          message: result.message || 'Connection failed. Please check your credentials.' 
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

    try {
      // Simulate the scanning process with progress updates
      const progressInterval = setInterval(() => {
        setScanProgress(prev => {
          if (prev >= 90) {
            clearInterval(progressInterval);
            return 90;
          }
          return prev + 10;
        });
      }, 500);

      // Corrected API call to backend
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
        setScanResults({ error: 'Scan failed. Please try again.' });
      }
    } catch (error) {
      setScanResults({ error: 'Network error during scan. Please try again.' });
    } finally {
      setIsScanning(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900">
      <div className="container mx-auto px-4 py-8">
        {/* Header */}
        <div className="text-center mb-12">
          <div className="flex items-center justify-center mb-4">
            <Server className="w-12 h-12 text-purple-400 mr-3" />
            <h1 className="text-4xl font-bold text-white">ServiceNow Scanner</h1>
          </div>
          <p className="text-gray-300 text-lg">Detect stale ownership using AI-powered analysis</p>
        </div>

        {/* Main Card */}
        <div className="max-w-2xl mx-auto bg-white/10 backdrop-blur-lg rounded-2xl shadow-2xl border border-white/20">
          <div className="p-8">
            {/* Connection Form */}
            <div className="space-y-6 mb-8">
              <h2 className="text-2xl font-semibold text-white mb-6 flex items-center">
                <User className="w-6 h-6 mr-2 text-purple-400" />
                ServiceNow Connection
              </h2>

              {/* Instance URL */}
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

              {/* Username */}
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

              {/* Password */}
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

              {/* Test Connection Button */}
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

              {/* Connection Status */}
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
                <Search className="w-6 h-6 mr-2 text-purple-400" />
                Stale Ownership Analysis
              </h2>

              <div className="space-y-4">
                <p className="text-gray-300 text-sm">
                  Scan sys_audit and cmdb_ci tables to identify stale ownership patterns using your trained AI model.
                </p>

                {/* Scan Button */}
                <button
                  onClick={scanStaleOwnership}
                  disabled={isScanning || !connectionStatus?.success}
                  className="w-full bg-gradient-to-r from-purple-600 to-purple-700 hover:from-purple-700 hover:to-purple-800 disabled:from-gray-600 disabled:to-gray-700 text-white font-semibold py-3 px-6 rounded-lg transition-all duration-200 flex items-center justify-center"
                >
                  {isScanning ? (
                    <>
                      <Loader2 className="w-5 h-5 mr-2 animate-spin" />
                      Scanning... {scanProgress}%
                    </>
                  ) : (
                    <>
                      <Search className="w-5 h-5 mr-2" />
                      Scan for Stale Ownership
                    </>
                  )}
                </button>

                {/* Progress Bar */}
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
                  <div className="mt-6 p-6 bg-white/5 rounded-lg border border-white/10">
                    <h3 className="text-lg font-semibold text-white mb-4">Scan Results</h3>
                    {scanResults.error ? (
                      <div className="flex items-center text-red-400">
                        <AlertCircle className="w-5 h-5 mr-2" />
                        {scanResults.error}
                      </div>
                    ) : (
                      <div className="space-y-6">
                        {scanResults.tables && Object.entries(scanResults.tables).map(([table, info]) => (
                          <div key={table} className="bg-white/10 p-4 rounded-lg border border-white/10">
                            <div className="flex items-center mb-2">
                              <span className="text-purple-300 font-semibold mr-2">{table}</span>
                              <span className="text-gray-400 text-xs">({info.record_count} records)</span>
                            </div>
                            <div className="text-gray-300 text-xs mb-1">Fields:</div>
                            <div className="flex flex-wrap gap-2 mb-2">
                              {info.field_names && info.field_names.length > 0 ? (
                                info.field_names.map((field) => (
                                  <span key={field} className="bg-purple-900/40 text-purple-200 px-2 py-1 rounded text-xs border border-purple-700/30">{field}</span>
                                ))
                              ) : (
                                <span className="text-gray-400">No fields found</span>
                              )}
                            </div>
                            {/* Sample Records Table */}
                            <div className="overflow-x-auto mt-2">
                              {info.sample_records && info.sample_records.length > 0 ? (
                                <table className="min-w-full text-xs text-left text-gray-200 border border-purple-900/30">
                                  <thead>
                                    <tr>
                                      {info.field_names.map((field) => (
                                        <th key={field} className="px-2 py-1 border-b border-purple-900/30 bg-purple-950/40">{field}</th>
                                      ))}
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {info.sample_records.map((record, idx) => (
                                      <tr key={idx} className="hover:bg-purple-900/20">
                                        {info.field_names.map((field) => (
                                          <td key={field} className="px-2 py-1 border-b border-purple-900/30">
                                            {record[field] !== undefined && record[field] !== null ? String(record[field]) : <span className="text-gray-500">-</span>}
                                          </td>
                                        ))}
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              ) : (
                                <div className="text-gray-400 italic">No sample records found.</div>
                              )}
                            </div>
                          </div>
                        ))}
                        <div className="text-green-400 text-sm">
                          ✓ Analysis complete. Detailed report available for download.
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="text-center mt-8 text-gray-400 text-sm">
          <p>Powered by AI-driven ownership analysis • Secure ServiceNow integration</p>
        </div>
      </div>
    </div>
  );
};

export default ServiceNowScanner;