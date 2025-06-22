import React, { useState, useEffect } from 'react';
import { AlertCircle, CheckCircle, Loader2, Search, Server, User, Eye, EyeOff, Brain, TrendingUp, AlertTriangle, Users, ChevronDown, ChevronRight, Clock, Shield, Activity, UserCheck, Zap, Database, ChevronUp, ChevronLeft, MoreHorizontal } from 'lucide-react';

// Aceternity UI Components

// Background Beams Component
const BackgroundBeams = () => {
  return (
    <div className="absolute inset-0 overflow-hidden">
      <svg
        className="absolute inset-0 h-full w-full"
        width="100%"
        height="100%"
        xmlns="http://www.w3.org/2000/svg"
      >
        <defs>
          <pattern
            id="beams"
            x="0"
            y="0"
            width="100"
            height="100"
            patternUnits="userSpaceOnUse"
          >
            <g className="opacity-20">
              <circle cx="50" cy="50" r="1" fill="url(#gradient)" />
            </g>
          </pattern>
          <radialGradient id="gradient" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stopColor="#8B5CF6" stopOpacity="1" />
            <stop offset="100%" stopColor="#3B82F6" stopOpacity="0" />
          </radialGradient>
        </defs>
        <rect width="100%" height="100%" fill="url(#beams)" />
        <g className="animate-pulse">
          <circle cx="20%" cy="20%" r="2" fill="#8B5CF6" opacity="0.6" />
          <circle cx="80%" cy="30%" r="1.5" fill="#3B82F6" opacity="0.4" />
          <circle cx="40%" cy="70%" r="1" fill="#06B6D4" opacity="0.8" />
          <circle cx="90%" cy="80%" r="2.5" fill="#8B5CF6" opacity="0.3" />
          <circle cx="10%" cy="90%" r="1.5" fill="#3B82F6" opacity="0.5" />
        </g>
      </svg>
    </div>
  );
};

// Floating Navbar Component
const FloatingNav = ({ className = "" }) => {
  return (
    <div className={`fixed top-4 inset-x-0 max-w-md mx-auto z-50 ${className}`}>
      <div className="relative rounded-full border border-white/20 bg-black/20 backdrop-blur-md shadow-lg">
        <div className="flex items-center justify-center px-8 py-4">
          <div className="flex items-center space-x-2">
            <div className="h-8 w-8 rounded-full bg-gradient-to-r from-purple-500 to-blue-500 flex items-center justify-center">
              <Server className="h-4 w-4 text-white" />
            </div>
            <span className="text-white font-semibold">ServiceNow Scanner</span>
          </div>
        </div>
      </div>
    </div>
  );
};

// Spotlight Component
const Spotlight = ({ className = "", fill = "#8B5CF6" }) => {
  return (
    <svg
      className={`animate-pulse ${className}`}
      width="100%"
      height="100%"
      viewBox="0 0 400 400"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <g clipPath="url(#clip)">
        <g filter="url(#filter)">
          <circle cx="200" cy="200" r="200" fill={fill} fillOpacity="0.2" />
        </g>
      </g>
      <defs>
        <filter id="filter" x="-200%" y="-200%" width="400%" height="400%">
          <feGaussianBlur stdDeviation="200" result="effect1" />
        </filter>
        <clipPath id="clip">
          <rect width="400" height="400" />
        </clipPath>
      </defs>
    </svg>
  );
};

// Card Hover Effect Component
const CardContainer = ({ children, className = "" }) => {
  return (
    <div className={`group/card ${className}`} style={{ perspective: "1000px" }}>
      <div className="relative h-full w-full rounded-xl bg-gradient-to-br from-black/40 to-purple-900/20 backdrop-blur-sm border border-white/20 transition-all duration-500 group-hover/card:shadow-2xl group-hover/card:shadow-purple-500/25" style={{ transformStyle: "preserve-3d" }}>
        {children}
      </div>
    </div>
  );
};

// Button Component with Shimmer Effect
const ShimmerButton = ({ children, onClick, disabled, className = "", variant = "primary" }) => {
  const variants = {
    primary: "bg-gradient-to-r from-purple-500 to-blue-600 hover:from-purple-600 hover:to-blue-700",
    secondary: "bg-gradient-to-r from-blue-500 to-cyan-600 hover:from-blue-600 hover:to-cyan-700"
  };

  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`relative inline-flex h-12 w-full overflow-hidden rounded-lg p-[1px] focus:outline-none focus:ring-2 focus:ring-slate-400 focus:ring-offset-2 focus:ring-offset-slate-50 disabled:opacity-50 ${className}`}
    >
      <span className="absolute inset-[-1000%] animate-[spin_2s_linear_infinite] bg-[conic-gradient(from_90deg_at_50%_50%,#E2E8F0_0%,#393BB2_50%,#E2E8F0_100%)]" />
      <span className={`inline-flex h-full w-full cursor-pointer items-center justify-center rounded-lg ${variants[variant]} px-6 py-1 text-sm font-medium text-white backdrop-blur-3xl disabled:cursor-not-allowed`}>
        {children}
      </span>
    </button>
  );
};

// Label Component
const Label = ({ children, className = "" }) => {
  return (
    <label className={`text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70 text-white ${className}`}>
      {children}
    </label>
  );
};

// Input Component
const Input = ({ className = "", type = "text", ...props }) => {
  return (
    <input
      type={type}
      className={`flex h-10 w-full rounded-md border border-white/20 bg-black/20 backdrop-blur-sm px-3 py-2 text-sm text-white placeholder:text-gray-400 focus:outline-none focus:ring-2 focus:ring-purple-500 focus:ring-offset-2 focus:ring-offset-black disabled:cursor-not-allowed disabled:opacity-50 transition-all duration-200 ${className}`}
      {...props}
    />
  );
};

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
  const [showGroupedView, setShowGroupedView] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [itemsPerPage] = useState(50);
  const [expandedAlternates, setExpandedAlternates] = useState(new Set());
  const [activeFilter, setActiveFilter] = useState('all'); // 'all', 'critical', 'high', 'medium', 'low'

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

  // Pagination logic
  const getPaginatedCIs = () => {
    const filteredCIs = getFilteredCIs();
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    return filteredCIs.slice(startIndex, endIndex);
  };

  const getFilteredCIs = () => {
    if (!scanResults || !scanResults.stale_cis || !Array.isArray(scanResults.stale_cis)) return [];
    
    if (activeFilter === 'all') {
      return scanResults.stale_cis;
    }
    
    return scanResults.stale_cis.filter(ci => {
      const riskLevel = ci.risk_level ? ci.risk_level.toLowerCase() : '';
      return riskLevel === activeFilter;
    });
  };

  const getTotalPages = () => {
    const filteredCIs = getFilteredCIs();
    if (!filteredCIs || filteredCIs.length === 0) return 0;
    return Math.ceil(filteredCIs.length / itemsPerPage);
  };

  const setFilter = (filter) => {
    setActiveFilter(filter);
    setCurrentPage(1); // Reset to first page when filter changes
    setExpandedCI(null); // Close any expanded CI
    setExpandedAlternates(new Set()); // Clear expanded alternates
  };

  const goToPage = (page) => {
    setCurrentPage(page);
    setExpandedCI(null); // Close any expanded CI when changing pages
  };

  const goToNextPage = () => {
    if (currentPage < getTotalPages()) {
      goToPage(currentPage + 1);
    }
  };

  const goToPrevPage = () => {
    if (currentPage > 1) {
      goToPage(currentPage - 1);
    }
  };

  const toggleAlternateRecommendations = (ciId) => {
    const newExpanded = new Set(expandedAlternates);
    if (newExpanded.has(ciId)) {
      newExpanded.delete(ciId);
    } else {
      newExpanded.add(ciId);
    }
    setExpandedAlternates(newExpanded);
  };

  // Reset pagination when new results come in
  useEffect(() => {
    setCurrentPage(1);
    setExpandedCI(null);
    setExpandedAlternates(new Set());
    setActiveFilter('all');
  }, [scanResults]);

  return (
    <div className="min-h-screen bg-black relative overflow-hidden">
      {/* Fixed black background to prevent white overflow */}
      <div className="fixed inset-0 bg-black -z-10"></div>
      
      {/* Background Elements */}
      <div className="absolute inset-0 min-h-full bg-black">
        <div className="absolute top-40 left-40">
          <Spotlight className="w-96 h-96" fill="#8B5CF6" />
        </div>
        <div className="absolute bottom-40 right-40">
          <Spotlight className="w-96 h-96" fill="#3B82F6" />
        </div>
        <BackgroundBeams />
      </div>

      {/* Floating Navigation */}
      <FloatingNav />

      <div className="relative z-10 pt-24 pb-12">
        <div className="container mx-auto px-4">
          {/* Hero Section */}
          <div className="text-center mb-16">
            <div className="relative">
              <h1 className="text-6xl md:text-8xl font-bold text-transparent bg-clip-text bg-gradient-to-b from-neutral-50 to-neutral-400 mb-6">
                AI-Powered
                <br />
                <span className="text-transparent bg-clip-text bg-gradient-to-r from-purple-500 via-blue-500 to-cyan-500">
                  Scanner
                </span>
              </h1>
              <div className="absolute -top-4 left-1/2 transform -translate-x-1/2">
                <div className="w-2 h-2 bg-purple-500 rounded-full animate-ping"></div>
              </div>
            </div>
            <p className="text-xl text-gray-400 max-w-3xl mx-auto mb-8">
              Detect stale ownership using machine learning analysis with advanced AI algorithms
            </p>
            
            {/* Feature Cards */}
            <div className="flex flex-wrap justify-center gap-4 mb-12">
              {[
                { icon: Brain, text: "Machine Learning", color: "from-purple-500 to-pink-500" },
                { icon: Zap, text: "Real-time Analysis", color: "from-yellow-500 to-orange-500" },
                { icon: Shield, text: "Enterprise Security", color: "from-green-500 to-emerald-500" }
              ].map(({ icon: Icon, text, color }) => (
                <div key={text} className="group relative">
                  <div className={`absolute inset-0 bg-gradient-to-r ${color} rounded-full blur opacity-25 group-hover:opacity-40 transition-opacity`}></div>
                  <div className="relative bg-black/40 backdrop-blur-sm border border-white/20 rounded-full px-6 py-3 flex items-center space-x-3">
                    <Icon className="w-5 h-5 text-white" />
                    <span className="text-white font-medium">{text}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Main Content */}
          <div className="max-w-6xl mx-auto space-y-8">
            {/* Connection Form */}
            <CardContainer>
              <div className="p-8">
                <div className="flex items-center mb-8">
                  <div className="w-12 h-12 rounded-xl bg-gradient-to-r from-purple-500 to-blue-500 flex items-center justify-center mr-4">
                    <User className="w-6 h-6 text-white" />
                  </div>
                  <h2 className="text-3xl font-bold text-white">ServiceNow Connection</h2>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
                  <div>
                    <Label>Instance URL</Label>
                    <div className="relative mt-2">
                      <Server className="absolute left-3 top-3 w-4 h-4 text-gray-400" />
                      <Input
                        type="url"
                        name="instanceUrl"
                        value={formData.instanceUrl}
                        onChange={handleInputChange}
                        placeholder="https://your-instance.service-now.com"
                        className="pl-10"
                      />
                    </div>
                  </div>

                  <div>
                    <Label>Username</Label>
                    <div className="relative mt-2">
                      <User className="absolute left-3 top-3 w-4 h-4 text-gray-400" />
                      <Input
                        type="text"
                        name="username"
                        value={formData.username}
                        onChange={handleInputChange}
                        placeholder="Enter username"
                        className="pl-10"
                      />
                    </div>
                  </div>

                  <div>
                    <Label>Password</Label>
                    <div className="relative mt-2">
                      <div className="absolute left-3 top-3 w-4 h-4 text-gray-400">
                        🔒
                      </div>
                      <Input
                        type={showPassword ? 'text' : 'password'}
                        name="password"
                        value={formData.password}
                        onChange={handleInputChange}
                        placeholder="Enter password"
                        className="pl-10 pr-10"
                      />
                      <button
                        type="button"
                        onClick={() => setShowPassword(!showPassword)}
                        className="absolute right-3 top-3 text-gray-400 hover:text-white transition-colors"
                      >
                        {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                      </button>
                    </div>
                  </div>
                </div>

                <div className="mb-6">
                  <ShimmerButton
                    onClick={testConnection}
                    disabled={isConnecting}
                    variant="secondary"
                  >
                    {isConnecting ? (
                      <>
                        <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                        Testing Connection...
                      </>
                    ) : (
                      <>
                        <Server className="w-4 h-4 mr-2" />
                        Test Connection
                      </>
                    )}
                  </ShimmerButton>
                </div>

                {connectionStatus && (
                  <div className={`p-4 rounded-lg border backdrop-blur-sm ${
                    connectionStatus.success 
                      ? 'bg-green-500/10 border-green-500/30' 
                      : 'bg-red-500/10 border-red-500/30'
                  }`}>
                    <div className="flex items-start">
                      {connectionStatus.success ? (
                        <CheckCircle className="w-5 h-5 text-green-400 mr-3 mt-0.5" />
                      ) : (
                        <AlertCircle className="w-5 h-5 text-red-400 mr-3 mt-0.5" />
                      )}
                      <p className={`font-medium ${connectionStatus.success ? 'text-green-200' : 'text-red-200'}`}>
                        {connectionStatus.message}
                      </p>
                    </div>
                  </div>
                )}
              </div>
            </CardContainer>

            {/* Scan Section */}
            <CardContainer>
              <div className="p-8">
                <div className="flex items-center mb-6">
                  <div className="w-12 h-12 rounded-xl bg-gradient-to-r from-cyan-500 to-purple-500 flex items-center justify-center mr-4">
                    <Brain className="w-6 h-6 text-white" />
                  </div>
                  <h2 className="text-3xl font-bold text-white">AI-Powered Stale Ownership Analysis</h2>
                </div>

                <p className="text-gray-400 mb-6">
                  Our machine learning model analyzes audit trails, user activity patterns, and ownership changes to identify stale assignments with high accuracy.
                </p>

                <div className="mb-6">
                  <ShimmerButton
                    onClick={scanStaleOwnership}
                    disabled={isScanning || !connectionStatus?.success}
                    variant="primary"
                  >
                    {isScanning ? (
                      <>
                        <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                        AI Analysis in Progress... {scanProgress}%
                      </>
                    ) : (
                      <>
                        <Search className="w-4 h-4 mr-2" />
                        Start AI Analysis
                      </>
                    )}
                  </ShimmerButton>
                </div>

                {isScanning && (
                  <div className="mb-6">
                    <div className="w-full bg-white/10 rounded-full h-2 overflow-hidden">
                      <div 
                        className="h-full bg-gradient-to-r from-purple-500 to-cyan-500 rounded-full transition-all duration-300"
                        style={{ width: `${scanProgress}%` }}
                      />
                    </div>
                    <p className="text-center text-gray-400 text-xs mt-2">
                      Analyzing patterns...
                    </p>
                  </div>
                )}
              </div>
            </CardContainer>

            {/* Scan Results */}
            {scanResults && (
              <div className="space-y-6">
                {scanResults.error ? (
                  <CardContainer>
                    <div className="p-8">
                      <div className="flex items-center p-6 bg-red-500/10 border border-red-500/30 rounded-lg">
                        <AlertCircle className="w-6 h-6 text-red-400 mr-3" />
                        <span className="text-red-200 font-medium">{scanResults.error}</span>
                      </div>
                    </div>
                  </CardContainer>
                ) : (
                  <>
                    {/* Summary Cards */}
                    <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
                      {[
                        {
                          title: "Total CIs",
                          value: (scanResults && scanResults.summary) ? (scanResults.summary.total_cis_analyzed || 0) : 0,
                          icon: Server,
                          color: "text-blue-400",
                          filter: null, // Not filterable
                          clickable: false
                        },
                        {
                          title: "Stale Found",
                          value: (scanResults && scanResults.summary) ? (scanResults.summary.stale_cis_found || 0) : 0,
                          icon: AlertTriangle,
                          color: "text-red-400",
                          filter: "all",
                          clickable: true
                        },
                        {
                          title: "Critical Risk",
                          value: (scanResults && scanResults.summary) ? (scanResults.summary.critical_risk || 0) : 0,
                          icon: AlertCircle,
                          color: "text-red-400",
                          filter: "critical",
                          clickable: true
                        },
                        {
                          title: "High Risk",
                          value: (scanResults && scanResults.summary) ? (scanResults.summary.high_risk || 0) : 0,
                          icon: TrendingUp,
                          color: "text-orange-400",
                          filter: "high",
                          clickable: true
                        },
                        {
                          title: "Recommended Owners",
                          value: (scanResults && scanResults.summary) ? (scanResults.summary.recommended_owners_count || 0) : 0,
                          icon: Users,
                          color: "text-green-400",
                          filter: null, // Not filterable
                          clickable: false
                        }
                      ].map(({ title, value, icon: Icon, color, filter, clickable }) => (
                        <CardContainer key={title}>
                          <div 
                            className={`p-6 h-full flex items-center justify-between transition-all duration-200 ${
                              clickable 
                                ? `cursor-pointer hover:bg-white/5 ${
                                    activeFilter === filter ? 'bg-white/10 ring-2 ring-purple-500/50' : ''
                                  }` 
                                : ''
                            }`}
                            onClick={clickable ? () => setFilter(filter) : undefined}
                          >
                            <div>
                              <p className="text-gray-400 text-sm">{title}</p>
                              <p className={`text-2xl font-bold ${color}`}>{value}</p>
                              {clickable && (
                                <p className="text-xs text-gray-500 mt-1">
                                  {activeFilter === filter ? 'Filtered' : 'Click to filter'}
                                </p>
                              )}
                            </div>
                            <Icon className={`w-8 h-8 ${color}`} />
                          </div>
                        </CardContainer>
                      ))}
                    </div>

                    {/* Toggle Button for Grouped View */}
                    {scanResults && scanResults.grouped_by_owners && Array.isArray(scanResults.grouped_by_owners) && scanResults.grouped_by_owners.length > 0 && (
                      <div className="flex justify-center mb-6">
                        <button
                          onClick={() => setShowGroupedView(!showGroupedView)}
                          className="flex items-center space-x-2 px-6 py-3 bg-gradient-to-r from-blue-500 to-purple-600 hover:from-blue-600 hover:to-purple-700 text-white font-medium rounded-lg transition-all duration-200 transform hover:scale-105 shadow-lg"
                        >
                          <Users className="w-5 h-5" />
                          <span>{showGroupedView ? 'Hide' : 'Show'} Grouped by Owners</span>
                          {showGroupedView ? 
                            <ChevronUp className="w-4 h-4" /> : 
                            <ChevronDown className="w-4 h-4" />
                          }
                        </button>
                      </div>
                    )}

                    {/* Stale CIs List */}
                    <CardContainer>
                      <div className="overflow-hidden">
                        <div className="p-6 border-b border-white/10">
                          <div className="flex items-center justify-between">
                            <div>
                              <h3 className="text-2xl font-bold text-white flex items-center">
                                <Database className="w-6 h-6 mr-3 text-purple-400" />
                                Stale Configuration Items
                                {activeFilter !== 'all' && (
                                  <span className="ml-3 px-3 py-1 bg-purple-500/20 text-purple-300 rounded-full text-sm font-medium">
                                    {activeFilter.charAt(0).toUpperCase() + activeFilter.slice(1)} Risk Only
                                  </span>
                                )}
                              </h3>
                              <p className="text-gray-400 mt-2">
                                {activeFilter === 'all' 
                                  ? 'Click on any CI to view detailed analysis and recommendations' 
                                  : `Showing ${getFilteredCIs().length} ${activeFilter} risk CIs. Click summary cards above to change filter.`
                                }
                              </p>
                            </div>
                            
                            {activeFilter !== 'all' && (
                              <button
                                onClick={() => setFilter('all')}
                                className="px-4 py-2 bg-white/10 hover:bg-white/20 text-gray-300 hover:text-white rounded-lg transition-all text-sm flex items-center space-x-2"
                              >
                                <span>Clear Filter</span>
                                <span className="text-xs bg-white/20 px-2 py-1 rounded">Show All</span>
                              </button>
                            )}
                          </div>
                        </div>
                        
                        {scanResults && scanResults.stale_cis && scanResults.stale_cis.length > 0 ? (
                          <div className="space-y-0">
                            {getPaginatedCIs().map((ci, idx) => (
                              <div key={ci.ci_id} className="border-b border-white/10 last:border-b-0">
                                {/* CI List Item */}
                                <div 
                                  className="p-6 hover:bg-white/5 cursor-pointer transition-colors"
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
                                          {ci.staleness_reasons && ci.staleness_reasons.map((reason, reasonIdx) => (
                                            <div key={reasonIdx} className="bg-white/5 p-3 rounded-lg border border-white/10">
                                              <div className="flex items-center justify-between mb-2">
                                                <span className="text-purple-300 font-medium">
                                                  {reason.rule_name && reason.rule_name.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                                                </span>
                                                <span className="text-xs text-gray-400">
                                                  {reason.confidence && (reason.confidence * 100).toFixed(0)}% confidence
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
                                              <span className="text-white">{ci.owner_activity_count || 0}</span>
                                            </div>
                                            <div>
                                              <span className="text-gray-400">Last Activity: </span>
                                              <span className="text-white">
                                                {ci.days_since_owner_activity === 999 ? 'Never' : `${ci.days_since_owner_activity || 0} days ago`}
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
                                            {/* Primary Recommendation */}
                                            <div className="bg-white/5 p-3 rounded-lg border border-white/10">
                                              <div className="flex items-center justify-between mb-2">
                                                <div>
                                                  <div className="text-green-300 font-medium">{ci.recommended_owners[0].display_name}</div>
                                                  <div className="text-xs text-gray-400">{ci.recommended_owners[0].username}</div>
                                                </div>
                                                <div className="text-right">
                                                  <div className="text-green-400 font-bold">{ci.recommended_owners[0].score}/100</div>
                                                  <div className="text-xs text-gray-400">Top Match</div>
                                                </div>
                                              </div>
                                              
                                              <div className="grid grid-cols-2 gap-2 text-xs text-gray-300">
                                                <div>
                                                  <span className="text-gray-400">Activities: </span>
                                                  {ci.recommended_owners[0].activity_count}
                                                </div>
                                                <div>
                                                  <span className="text-gray-400">Last Seen: </span>
                                                  {ci.recommended_owners[0].last_activity_days_ago === 999 ? 'Never' : `${ci.recommended_owners[0].last_activity_days_ago}d ago`}
                                                </div>
                                                <div>
                                                  <span className="text-gray-400">Ownership Changes: </span>
                                                  {ci.recommended_owners[0].ownership_changes}
                                                </div>
                                                <div>
                                                  <span className="text-gray-400">Fields Modified: </span>
                                                  {ci.recommended_owners[0].fields_modified}
                                                </div>
                                              </div>
                                              
                                              {ci.recommended_owners[0].department && (
                                                <div className="mt-2 text-xs text-gray-400">
                                                  Department: {ci.recommended_owners[0].department}
                                                </div>
                                              )}
                                            </div>

                                            {/* Show Alternate Recommendations Button */}
                                            {ci.recommended_owners.length >= 1 && (
                                              <div className="text-center">
                                                <button
                                                  onClick={() => toggleAlternateRecommendations(ci.ci_id)}
                                                  className="flex items-center space-x-2 mx-auto px-4 py-2 bg-white/10 hover:bg-white/20 text-gray-300 hover:text-white rounded-lg transition-all text-sm"
                                                >
                                                  <MoreHorizontal className="w-4 h-4" />
                                                  <span>
                                                    {expandedAlternates.has(ci.ci_id) ? 'Hide' : 'Show'} {ci.recommended_owners.length > 1 ? 'Alternate' : 'All'} Recommendations
                                                  </span>
                                                  <span className="bg-white/20 px-2 py-1 rounded text-xs">
                                                    {ci.recommended_owners.length > 1 ? `+${ci.recommended_owners.length - 1}` : `2 total`}
                                                  </span>
                                                </button>
                                              </div>
                                            )}

                                            {/* Alternate Recommendations */}
                                            {expandedAlternates.has(ci.ci_id) && ci.recommended_owners.length >= 1 && (
                                              <div className="space-y-2 border-t border-white/10 pt-3">
                                                <div className="text-sm text-gray-400 font-medium mb-2">
                                                  {ci.recommended_owners.length > 1 ? 'Alternate Options:' : 'All Recommendations:'}
                                                </div>
                                                {/* Show actual alternates for multiple recommendations */}
                                                {ci.recommended_owners.length > 1 && ci.recommended_owners.slice(1).map((owner, ownerIdx) => (
                                                  <div key={ownerIdx + 1} className="bg-white/3 p-3 rounded-lg border border-white/5">
                                                    <div className="flex items-center justify-between mb-2">
                                                      <div>
                                                        <div className="text-blue-300 font-medium">{owner.display_name}</div>
                                                        <div className="text-xs text-gray-400">{owner.username}</div>
                                                      </div>
                                                      <div className="text-right">
                                                        <div className="text-blue-400 font-bold">{owner.score}/100</div>
                                                        <div className="text-xs text-gray-400">#{ownerIdx + 2} Match</div>
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

                                                {/* Show current owner as alternate when only 1 recommendation */}
                                                {ci.recommended_owners.length === 1 && (
                                                  <div className="bg-white/3 p-3 rounded-lg border border-white/5">
                                                    <div className="flex items-center justify-between mb-2">
                                                      <div>
                                                        <div className="text-orange-300 font-medium">{ci.current_owner} (Current)</div>
                                                        <div className="text-xs text-gray-400">{ci.current_owner}</div>
                                                      </div>
                                                      <div className="text-right">
                                                        <div className="text-orange-400 font-bold">0/100</div>
                                                        <div className="text-xs text-gray-400">Current Owner</div>
                                                      </div>
                                                    </div>
                                                    
                                                    <div className="grid grid-cols-2 gap-2 text-xs text-gray-300">
                                                      <div>
                                                        <span className="text-gray-400">Activities: </span>
                                                        {ci.owner_activity_count || 0}
                                                      </div>
                                                      <div>
                                                        <span className="text-gray-400">Last Seen: </span>
                                                        {ci.days_since_owner_activity === 999 ? 'Never' : `${ci.days_since_owner_activity || 0}d ago`}
                                                      </div>
                                                      <div>
                                                        <span className="text-gray-400">Status: </span>
                                                        <span className={ci.owner_active ? 'text-green-400' : 'text-red-400'}>
                                                          {ci.owner_active ? 'Active' : 'Inactive'}
                                                        </span>
                                                      </div>
                                                      <div>
                                                        <span className="text-gray-400">Risk: </span>
                                                        <span className="text-red-400">Stale Ownership</span>
                                                      </div>
                                                    </div>
                                                    
                                                    <div className="mt-2 text-xs text-orange-400">
                                                      Note: Current owner marked as stale - consider reassignment
                                                    </div>
                                                  </div>
                                                )}
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

                        {/* Pagination Controls */}
                        {getFilteredCIs().length > itemsPerPage && (
                          <div className="p-6 border-t border-white/10 bg-white/2">
                            <div className="flex items-center justify-between">
                              <div className="text-sm text-gray-400">
                                Showing {((currentPage - 1) * itemsPerPage) + 1} to {Math.min(currentPage * itemsPerPage, getFilteredCIs().length)} of {getFilteredCIs().length} {activeFilter === 'all' ? 'stale CIs' : `${activeFilter} risk CIs`}
                                {activeFilter !== 'all' && scanResults && scanResults.stale_cis && (
                                  <span className="ml-2">
                                    (filtered from {scanResults.stale_cis.length} total)
                                  </span>
                                )}
                              </div>
                              
                              <div className="flex items-center space-x-2">
                                {/* Previous Button */}
                                <button
                                  onClick={goToPrevPage}
                                  disabled={currentPage === 1}
                                  className={`px-3 py-2 rounded-lg text-sm font-medium transition-all ${
                                    currentPage === 1 
                                      ? 'bg-gray-800 text-gray-500 cursor-not-allowed' 
                                      : 'bg-white/10 text-white hover:bg-white/20 hover:scale-105'
                                  }`}
                                >
                                  <ChevronLeft className="w-4 h-4" />
                                </button>

                                {/* Page Numbers */}
                                <div className="flex items-center space-x-1">
                                  {getTotalPages() > 0 && Array.from({ length: Math.min(5, getTotalPages()) }, (_, i) => {
                                    let pageNum;
                                    if (getTotalPages() <= 5) {
                                      pageNum = i + 1;
                                    } else if (currentPage <= 3) {
                                      pageNum = i + 1;
                                    } else if (currentPage >= getTotalPages() - 2) {
                                      pageNum = getTotalPages() - 4 + i;
                                    } else {
                                      pageNum = currentPage - 2 + i;
                                    }

                                    return (
                                      <button
                                        key={pageNum}
                                        onClick={() => goToPage(pageNum)}
                                        className={`px-3 py-2 rounded-lg text-sm font-medium transition-all ${
                                          currentPage === pageNum
                                            ? 'bg-gradient-to-r from-blue-500 to-purple-600 text-white shadow-lg'
                                            : 'bg-white/10 text-gray-300 hover:bg-white/20 hover:text-white'
                                        }`}
                                      >
                                        {pageNum}
                                      </button>
                                    );
                                  })}
                                  
                                  {getTotalPages() > 5 && currentPage < getTotalPages() - 2 && (
                                    <>
                                      <span className="text-gray-400 px-2">...</span>
                                      <button
                                        onClick={() => goToPage(getTotalPages())}
                                        className="px-3 py-2 rounded-lg text-sm font-medium bg-white/10 text-gray-300 hover:bg-white/20 hover:text-white transition-all"
                                      >
                                        {getTotalPages()}
                                      </button>
                                    </>
                                  )}
                                </div>

                                {/* Next Button */}
                                <button
                                  onClick={goToNextPage}
                                  disabled={currentPage === getTotalPages()}
                                  className={`px-3 py-2 rounded-lg text-sm font-medium transition-all ${
                                    currentPage === getTotalPages() 
                                      ? 'bg-gray-800 text-gray-500 cursor-not-allowed' 
                                      : 'bg-white/10 text-white hover:bg-white/20 hover:scale-105'
                                  }`}
                                >
                                  <ChevronRight className="w-4 h-4" />
                                </button>
                              </div>
                            </div>
                          </div>
                        )}
                      </div>
                    </CardContainer>

                    {/* Grouped by Recommended Owners */}
                    {scanResults && scanResults.grouped_by_owners && Array.isArray(scanResults.grouped_by_owners) && scanResults.grouped_by_owners.length > 0 && showGroupedView && (
                      <CardContainer>
                        <div className="overflow-hidden">
                          <div className="p-6 border-b border-white/10">
                            <h3 className="text-2xl font-bold text-white flex items-center">
                              <Users className="w-6 h-6 mr-3 text-blue-400" />
                              Grouped by Recommended Owners
                            </h3>
                            <p className="text-gray-400 mt-2">CIs grouped by recommended assignees for bulk operations</p>
                          </div>
                          
                          <div className="space-y-4 p-6">
                            {scanResults.grouped_by_owners.map((group, idx) => (
                              <div key={group.username} className="bg-white/5 rounded-lg border border-white/10 overflow-hidden">
                                {/* Owner Header */}
                                <div className="p-4 bg-white/5 border-b border-white/10">
                                  <div className="flex items-center justify-between">
                                    <div className="flex items-center space-x-4">
                                      <div className="w-12 h-12 rounded-full bg-gradient-to-r from-blue-500 to-purple-500 flex items-center justify-center">
                                        <UserCheck className="w-6 h-6 text-white" />
                                      </div>
                                      <div>
                                        <h4 className="text-white font-semibold text-lg">{group.recommended_owner.display_name}</h4>
                                        <div className="text-gray-400 text-sm">
                                          {group.recommended_owner.username} • {group.recommended_owner.department}
                                        </div>
                                      </div>
                                    </div>
                                    
                                    {/* Statistics */}
                                    <div className="text-right">
                                      <div className="text-2xl font-bold text-blue-400">{group.total_cis}</div>
                                      <div className="text-gray-400 text-sm">CIs to assign</div>
                                    </div>
                                  </div>
                                  
                                  {/* Statistics */}
                                  <div className="mt-4 grid grid-cols-2 md:grid-cols-4 gap-4">
                                    <div className="text-center">
                                      <div className="text-lg font-semibold text-green-400">{group.recommended_owner.avg_score}</div>
                                      <div className="text-xs text-gray-400">Avg Score</div>
                                    </div>
                                    <div className="text-center">
                                      <div className="text-lg font-semibold text-purple-400">{(group.avg_confidence * 100).toFixed(0)}%</div>
                                      <div className="text-xs text-gray-400">Avg Confidence</div>
                                    </div>
                                    <div className="text-center">
                                      <div className="text-lg font-semibold text-red-400">{group.risk_breakdown.Critical + group.risk_breakdown.High}</div>
                                      <div className="text-xs text-gray-400">High Risk</div>
                                    </div>
                                    <div className="text-center">
                                      <div className="text-lg font-semibold text-orange-400">{group.recommended_owner.total_activity_count}</div>
                                      <div className="text-xs text-gray-400">Total Activities</div>
                                    </div>
                                  </div>
                                </div>
                                
                                {/* CIs List */}
                                <div className="p-4">
                                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                                    {group.cis_to_assign.map((ci, ciIdx) => (
                                      <div key={ci.ci_id} className="bg-white/5 p-3 rounded-lg border border-white/10">
                                        <div className="flex items-center justify-between mb-2">
                                          <div className="font-medium text-white text-sm truncate">{ci.ci_name}</div>
                                          <span className={`px-2 py-1 text-xs rounded ${getRiskColor(ci.risk_level)}`}>
                                            {ci.risk_level}
                                          </span>
                                        </div>
                                        <div className="text-xs text-gray-400 mb-2">{ci.ci_class}</div>
                                        <div className="flex justify-between text-xs">
                                          <span className="text-gray-400">Current: {ci.current_owner}</span>
                                          <span className="text-green-400">{(ci.confidence * 100).toFixed(0)}%</span>
                                        </div>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      </CardContainer>
                    )}
                  </>
                )}
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="text-center mt-12">
            <div className="bg-white/5 backdrop-blur-sm rounded-xl p-6 border border-white/10 max-w-2xl mx-auto">
              <p className="text-gray-300">
                Powered by <span className="text-purple-400 font-semibold">advanced machine learning algorithms</span> • 
                <span className="text-blue-400 font-semibold"> Real-time ServiceNow integration</span>
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ServiceNowScanner;