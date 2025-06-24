import React, { useState, useEffect } from 'react';
import { AlertCircle, CheckCircle, Loader2, Search, Server, User, Eye, EyeOff, Brain, TrendingUp, AlertTriangle, Users, ChevronDown, ChevronRight, Clock, Shield, Activity, UserCheck, Zap, Database, ChevronUp, ChevronLeft, MoreHorizontal, UserPlus, CheckCircle2, X, ArrowUp, ArrowDown, History, RotateCcw } from 'lucide-react';
import { Listbox } from '@headlessui/react';
import config from './config.js';

// Error Boundary Component
class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    console.error('Error caught by boundary:', error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900 flex items-center justify-center">
          <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-6 max-w-md">
            <div className="flex items-center space-x-3 mb-4">
              <AlertCircle className="w-6 h-6 text-red-400" />
              <h3 className="text-lg font-semibold text-red-300">Something went wrong</h3>
            </div>
            <p className="text-red-200 text-sm mb-4">
              An error occurred while rendering the grouped view. Please try refreshing the page or switching to a different view.
            </p>
            <button
              onClick={() => this.setState({ hasError: false, error: null })}
              className="px-4 py-2 bg-red-500/20 hover:bg-red-500/30 border border-red-500/30 text-red-300 rounded-lg transition-colors"
            >
              Try Again
            </button>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

// Aceternity UI Components

// Background Beams Component with Glowing Stars
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
          {/* Star glow effects */}
          <filter id="starGlow">
            <feGaussianBlur stdDeviation="2" result="coloredBlur"/>
            <feMerge> 
              <feMergeNode in="coloredBlur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
          <filter id="smallStarGlow">
            <feGaussianBlur stdDeviation="1" result="coloredBlur"/>
            <feMerge> 
              <feMergeNode in="coloredBlur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
        </defs>
        <rect width="100%" height="100%" fill="url(#beams)" />
        
        {/* Animated beam circles */}
        <g className="animate-pulse">
          <circle cx="20%" cy="20%" r="2" fill="#8B5CF6" opacity="0.6" />
          <circle cx="80%" cy="30%" r="1.5" fill="#3B82F6" opacity="0.4" />
          <circle cx="40%" cy="70%" r="1" fill="#06B6D4" opacity="0.8" />
          <circle cx="90%" cy="80%" r="2.5" fill="#8B5CF6" opacity="0.3" />
          <circle cx="10%" cy="90%" r="1.5" fill="#3B82F6" opacity="0.5" />
        </g>
        
        {/* Glowing stars */}
        <g>
          {/* Large glowing stars */}
          <circle cx="15%" cy="15%" r="1" fill="#ffffff" opacity="0.9" filter="url(#starGlow)" className="animate-pulse" style={{ animationDuration: '3s', animationDelay: '0s' }} />
          <circle cx="85%" cy="25%" r="1" fill="#e0e7ff" opacity="0.8" filter="url(#starGlow)" className="animate-pulse" style={{ animationDuration: '4s', animationDelay: '1s' }} />
          <circle cx="25%" cy="45%" r="1" fill="#ddd6fe" opacity="0.7" filter="url(#starGlow)" className="animate-pulse" style={{ animationDuration: '3.5s', animationDelay: '2s' }} />
          <circle cx="70%" cy="55%" r="1" fill="#bfdbfe" opacity="0.9" filter="url(#starGlow)" className="animate-pulse" style={{ animationDuration: '2.8s', animationDelay: '0.5s' }} />
          <circle cx="10%" cy="75%" r="1" fill="#ffffff" opacity="0.8" filter="url(#starGlow)" className="animate-pulse" style={{ animationDuration: '3.2s', animationDelay: '1.5s' }} />
          <circle cx="90%" cy="85%" r="1" fill="#e0e7ff" opacity="0.6" filter="url(#starGlow)" className="animate-pulse" style={{ animationDuration: '4.2s', animationDelay: '2.5s' }} />
          <circle cx="55%" cy="12%" r="1" fill="#ffffff" opacity="0.7" filter="url(#starGlow)" className="animate-pulse" style={{ animationDuration: '3.8s', animationDelay: '1.8s' }} />
          <circle cx="8%" cy="40%" r="1" fill="#ddd6fe" opacity="0.8" filter="url(#starGlow)" className="animate-pulse" style={{ animationDuration: '2.9s', animationDelay: '3.2s' }} />
          <circle cx="92%" cy="65%" r="1" fill="#bfdbfe" opacity="0.6" filter="url(#starGlow)" className="animate-pulse" style={{ animationDuration: '3.7s', animationDelay: '0.8s' }} />
          <circle cx="42%" cy="88%" r="1" fill="#ffffff" opacity="0.9" filter="url(#starGlow)" className="animate-pulse" style={{ animationDuration: '4.1s', animationDelay: '2.2s' }} />
          
          {/* Medium glowing stars */}
          <circle cx="35%" cy="20%" r="0.5" fill="#ffffff" opacity="0.7" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.5s', animationDelay: '0.8s' }} />
          <circle cx="65%" cy="35%" r="0.5" fill="#e0e7ff" opacity="0.6" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.8s', animationDelay: '1.2s' }} />
          <circle cx="50%" cy="60%" r="0.5" fill="#ddd6fe" opacity="0.8" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.2s', animationDelay: '2.8s' }} />
          <circle cx="20%" cy="80%" r="0.5" fill="#bfdbfe" opacity="0.5" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.5s', animationDelay: '0.3s' }} />
          <circle cx="80%" cy="70%" r="0.5" fill="#ffffff" opacity="0.7" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.8s', animationDelay: '1.8s' }} />
          <circle cx="12%" cy="28%" r="0.5" fill="#e0e7ff" opacity="0.6" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.2s', animationDelay: '2.1s' }} />
          <circle cx="88%" cy="42%" r="0.5" fill="#ddd6fe" opacity="0.7" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.7s', animationDelay: '0.9s' }} />
          <circle cx="38%" cy="72%" r="0.5" fill="#bfdbfe" opacity="0.5" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.4s', animationDelay: '1.6s' }} />
          <circle cx="72%" cy="18%" r="0.5" fill="#ffffff" opacity="0.8" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.6s', animationDelay: '2.7s' }} />
          <circle cx="28%" cy="92%" r="0.5" fill="#e0e7ff" opacity="0.6" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.9s', animationDelay: '0.4s' }} />
          <circle cx="58%" cy="38%" r="0.5" fill="#ddd6fe" opacity="0.7" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.4s', animationDelay: '3.1s' }} />
          <circle cx="18%" cy="58%" r="0.5" fill="#bfdbfe" opacity="0.5" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.6s', animationDelay: '1.3s' }} />
          <circle cx="82%" cy="28%" r="0.5" fill="#ffffff" opacity="0.8" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.3s', animationDelay: '2.4s' }} />
          <circle cx="48%" cy="78%" r="0.5" fill="#e0e7ff" opacity="0.6" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.7s', animationDelay: '0.7s' }} />
          <circle cx="68%" cy="8%" r="0.5" fill="#ddd6fe" opacity="0.7" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.9s', animationDelay: '1.9s' }} />
          
          {/* Small twinkling stars */}
          <circle cx="5%" cy="35%" r="0.3" fill="#ffffff" opacity="0.5" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2s', animationDelay: '0.2s' }} />
          <circle cx="95%" cy="45%" r="0.3" fill="#e0e7ff" opacity="0.4" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.7s', animationDelay: '1.1s' }} />
          <circle cx="45%" cy="25%" r="0.3" fill="#ddd6fe" opacity="0.6" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.3s', animationDelay: '2.2s' }} />
          <circle cx="75%" cy="15%" r="0.3" fill="#bfdbfe" opacity="0.3" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.1s', animationDelay: '0.7s' }} />
          <circle cx="30%" cy="85%" r="0.3" fill="#ffffff" opacity="0.5" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.4s', animationDelay: '1.6s' }} />
          <circle cx="60%" cy="90%" r="0.3" fill="#e0e7ff" opacity="0.4" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.9s', animationDelay: '2.9s' }} />
          <circle cx="15%" cy="50%" r="0.3" fill="#ddd6fe" opacity="0.6" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.1s', animationDelay: '0.9s' }} />
          <circle cx="85%" cy="60%" r="0.3" fill="#bfdbfe" opacity="0.4" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.3s', animationDelay: '2.1s' }} />
          <circle cx="22%" cy="12%" r="0.3" fill="#ffffff" opacity="0.5" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.6s', animationDelay: '0.5s' }} />
          <circle cx="78%" cy="32%" r="0.3" fill="#e0e7ff" opacity="0.4" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.2s', animationDelay: '1.4s' }} />
          <circle cx="32%" cy="52%" r="0.3" fill="#ddd6fe" opacity="0.6" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.8s', animationDelay: '2.6s' }} />
          <circle cx="88%" cy="72%" r="0.3" fill="#bfdbfe" opacity="0.3" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.2s', animationDelay: '0.1s' }} />
          <circle cx="12%" cy="92%" r="0.3" fill="#ffffff" opacity="0.5" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.5s', animationDelay: '1.7s' }} />
          <circle cx="68%" cy="22%" r="0.3" fill="#e0e7ff" opacity="0.4" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.1s', animationDelay: '3.0s' }} />
          <circle cx="38%" cy="42%" r="0.3" fill="#ddd6fe" opacity="0.6" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.7s', animationDelay: '0.6s' }} />
          <circle cx="92%" cy="8%" r="0.3" fill="#bfdbfe" opacity="0.3" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.4s', animationDelay: '2.3s' }} />
          <circle cx="8%" cy="62%" r="0.3" fill="#ffffff" opacity="0.5" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.3s', animationDelay: '1.0s' }} />
          <circle cx="52%" cy="82%" r="0.3" fill="#e0e7ff" opacity="0.4" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.9s', animationDelay: '2.8s' }} />
          <circle cx="62%" cy="48%" r="0.3" fill="#ddd6fe" opacity="0.6" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.4s', animationDelay: '0.3s' }} />
          <circle cx="18%" cy="18%" r="0.3" fill="#bfdbfe" opacity="0.3" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.1s', animationDelay: '1.5s' }} />
          <circle cx="82%" cy="88%" r="0.3" fill="#ffffff" opacity="0.5" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.6s', animationDelay: '2.7s' }} />
          <circle cx="42%" cy="68%" r="0.3" fill="#e0e7ff" opacity="0.4" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.2s', animationDelay: '0.8s' }} />
          <circle cx="72%" cy="38%" r="0.3" fill="#ddd6fe" opacity="0.6" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.8s', animationDelay: '2.0s' }} />
          <circle cx="28%" cy="28%" r="0.3" fill="#bfdbfe" opacity="0.3" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.3s', animationDelay: '1.2s' }} />
          <circle cx="88%" cy="18%" r="0.3" fill="#ffffff" opacity="0.5" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.5s', animationDelay: '2.5s' }} />
          <circle cx="48%" cy="58%" r="0.3" fill="#e0e7ff" opacity="0.4" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.1s', animationDelay: '0.4s' }} />
          <circle cx="8%" cy="78%" r="0.3" fill="#ddd6fe" opacity="0.6" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.7s', animationDelay: '1.8s' }} />
          <circle cx="58%" cy="18%" r="0.3" fill="#bfdbfe" opacity="0.3" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.0s', animationDelay: '2.4s' }} />
          <circle cx="78%" cy="78%" r="0.3" fill="#ffffff" opacity="0.5" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.4s', animationDelay: '0.9s' }} />
          <circle cx="38%" cy="8%" r="0.3" fill="#e0e7ff" opacity="0.4" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.8s', animationDelay: '1.7s' }} />
          <circle cx="98%" cy="28%" r="0.3" fill="#ddd6fe" opacity="0.6" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.3s', animationDelay: '2.9s' }} />
          <circle cx="2%" cy="48%" r="0.3" fill="#bfdbfe" opacity="0.3" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.2s', animationDelay: '0.2s' }} />
          <circle cx="52%" cy="28%" r="0.3" fill="#ffffff" opacity="0.5" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.6s', animationDelay: '1.3s' }} />
          <circle cx="72%" cy="68%" r="0.3" fill="#e0e7ff" opacity="0.4" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.2s', animationDelay: '2.6s' }} />
          <circle cx="22%" cy="38%" r="0.3" fill="#ddd6fe" opacity="0.6" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.9s', animationDelay: '0.7s' }} />
          <circle cx="92%" cy="48%" r="0.3" fill="#bfdbfe" opacity="0.3" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.1s', animationDelay: '1.9s' }} />
          <circle cx="12%" cy="68%" r="0.3" fill="#ffffff" opacity="0.5" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.5s', animationDelay: '2.8s' }} />
          <circle cx="62%" cy="8%" r="0.3" fill="#e0e7ff" opacity="0.4" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.1s', animationDelay: '0.6s' }} />
          <circle cx="82%" cy="58%" r="0.3" fill="#ddd6fe" opacity="0.6" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.7s', animationDelay: '1.4s' }} />
          <circle cx="42%" cy="18%" r="0.3" fill="#bfdbfe" opacity="0.3" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '3.3s', animationDelay: '2.2s' }} />
          <circle cx="2%" cy="88%" r="0.3" fill="#ffffff" opacity="0.5" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.4s', animationDelay: '0.1s' }} />
          <circle cx="98%" cy="68%" r="0.3" fill="#e0e7ff" opacity="0.4" filter="url(#smallStarGlow)" className="animate-pulse" style={{ animationDuration: '2.8s', animationDelay: '1.6s' }} />
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
      className={`flex h-10 w-full rounded-md border border-white/20 bg-black/30 backdrop-blur-sm px-3 py-2 text-sm text-white placeholder:text-gray-600 focus:outline-none focus:ring-2 focus:ring-purple-500 focus:ring-offset-2 focus:ring-offset-black disabled:cursor-not-allowed disabled:opacity-50 transition-all duration-200 ${className}`}
      style={{
        backgroundColor: 'rgba(0, 0, 0, 0.3)',
        color: 'white',
        WebkitTextFillColor: 'white',
        WebkitBoxShadow: '0 0 0 1000px rgba(0, 0, 0, 0.3) inset',
        WebkitBackgroundClip: 'text'
      }}
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
  const [currentPage, setCurrentPage] = useState(1);
  const [itemsPerPage] = useState(50);
  const [expandedAlternates, setExpandedAlternates] = useState(new Set());
  const [activeFilter, setActiveFilter] = useState('all'); // 'all', 'critical', 'high', 'medium', 'low', 'by-owner'
  const [selectedOwnerFilter, setSelectedOwnerFilter] = useState('all'); // 'all' or specific owner username
  const [expandedOwner, setExpandedOwner] = useState(null); // Track which owner's CIs are expanded
  const [assigningCIs, setAssigningCIs] = useState(new Set()); // Track CIs being assigned
  const [assignmentResults, setAssignmentResults] = useState({}); // Track assignment results
  
  // New functionality state
  const [searchTerm, setSearchTerm] = useState(''); // Search functionality
  const [assignedCIs, setAssignedCIs] = useState([]); // Track successfully assigned CIs
  const [sortBy, setSortBy] = useState('confidence'); // 'confidence', 'risk', 'name', 'date'
  const [sortOrder, setSortOrder] = useState('desc'); // 'asc', 'desc'
  const [showAssignedSection, setShowAssignedSection] = useState(false); // Toggle assigned CIs section
  const [showAssignmentHistory, setShowAssignmentHistory] = useState(false); // Toggle assignment history
  const [assignmentHistory, setAssignmentHistory] = useState([]);
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);
  const [undoingAssignment, setUndoingAssignment] = useState(null);
  const [showScrollButtons, setShowScrollButtons] = useState(false);

  // Scroll functionality
  useEffect(() => {
    const handleScroll = () => {
      const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
      setShowScrollButtons(scrollTop > 300);
    };

    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  const scrollToTop = () => {
    window.scrollTo({
      top: 0,
      behavior: 'smooth'
    });
  };

  const scrollToBottom = () => {
    window.scrollTo({
      top: document.documentElement.scrollHeight,
      behavior: 'smooth'
    });
  };

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
      const response = await fetch(`${config.API_URL}/test-connection`, {
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

      const response = await fetch(`${config.API_URL}/scan-stale-ownership`, {
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
        // Scroll to top after scan completion
        setTimeout(() => {
          scrollToTop();
        }, 100);
      } else {
        const error = await response.json();
        setScanResults({ error: error.error || 'Scan failed. Please try again.' });
        // Scroll to top even on error
        setTimeout(() => {
          scrollToTop();
        }, 100);
      }
    } catch (error) {
      setScanResults({ error: 'Network error during scan. Please try again.' });
      // Scroll to top on network error
      setTimeout(() => {
        scrollToTop();
      }, 100);
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
    
    let filteredCIs = scanResults.stale_cis;
    
    // Apply owner-based filter (only high risk and critical)
    if (activeFilter === 'by-owner') {
      // First filter to only high risk and critical CIs
      filteredCIs = filteredCIs.filter(ci => 
        ci.risk_level === 'Critical' || ci.risk_level === 'High'
      );
      
      // Then filter by selected owner if not 'all'
      if (selectedOwnerFilter !== 'all') {
        filteredCIs = filteredCIs.filter(ci => 
          ci.recommended_owners && 
          ci.recommended_owners.length > 0 && 
          ci.recommended_owners[0].username === selectedOwnerFilter
        );
      }
    } else if (activeFilter !== 'all') {
      // Apply regular risk level filter
      filteredCIs = filteredCIs.filter(ci => {
        const riskLevel = ci.risk_level ? ci.risk_level.toLowerCase() : '';
        return riskLevel === activeFilter;
      });
    }
    
    // Apply search filter
    if (searchTerm) {
      filteredCIs = filteredCIs.filter(ci => {
        const searchLower = searchTerm.toLowerCase();
        return (
          ci.ci_name?.toLowerCase().includes(searchLower) ||
          ci.ci_class?.toLowerCase().includes(searchLower) ||
          ci.current_owner?.toLowerCase().includes(searchLower) ||
          ci.ci_description?.toLowerCase().includes(searchLower)
        );
      });
    }
    
    // Apply sorting
    filteredCIs.sort((a, b) => {
      let aValue, bValue;
      
      switch (sortBy) {
        case 'confidence':
          aValue = a.confidence || 0;
          bValue = b.confidence || 0;
          break;
        case 'risk':
          const riskOrder = { 'Critical': 4, 'High': 3, 'Medium': 2, 'Low': 1 };
          aValue = riskOrder[a.risk_level] || 0;
          bValue = riskOrder[b.risk_level] || 0;
          break;
        case 'name':
          aValue = a.ci_name?.toLowerCase() || '';
          bValue = b.ci_name?.toLowerCase() || '';
          break;
        case 'date':
          aValue = a.days_since_owner_activity || 999;
          bValue = b.days_since_owner_activity || 999;
          break;
        default:
          aValue = a.confidence || 0;
          bValue = b.confidence || 0;
      }
      
      if (sortOrder === 'asc') {
        return aValue > bValue ? 1 : aValue < bValue ? -1 : 0;
      } else {
        return aValue < bValue ? 1 : aValue > bValue ? -1 : 0;
      }
    });
    
    return filteredCIs;
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
    
    // Reset owner filter when switching away from by-owner
    if (filter !== 'by-owner') {
      setSelectedOwnerFilter('all');
    }
  };

  // Get recommended owners statistics for high risk and critical CIs
  const getRecommendedOwnersStats = () => {
    try {
      if (!scanResults || !scanResults.stale_cis || !Array.isArray(scanResults.stale_cis)) {
        console.log('No scan results available for owner stats');
        return [];
      }
      
      const highRiskCIs = scanResults.stale_cis.filter(ci => 
        ci && (ci.risk_level === 'Critical' || ci.risk_level === 'High')
      );
      
      console.log('High risk CIs for owner stats:', highRiskCIs.length);
      
      const ownerStats = {};
      
      highRiskCIs.forEach(ci => {
        try {
          if (ci.recommended_owners && Array.isArray(ci.recommended_owners) && ci.recommended_owners.length > 0) {
            const primaryOwner = ci.recommended_owners[0];
            if (primaryOwner && primaryOwner.username) {
              const mappedOwner = mapSystemToAdmin(primaryOwner.username, primaryOwner.display_name);
              const key = mappedOwner.username;
              
              if (!ownerStats[key]) {
                ownerStats[key] = {
                  username: mappedOwner.username,
                  display_name: mappedOwner.display_name,
                  total_cis: 0,
                  critical_count: 0,
                  high_count: 0,
                  avg_confidence: 0,
                  confidences: []
                };
              }
              
              ownerStats[key].total_cis++;
              ownerStats[key].confidences.push(ci.confidence || 0);
              
              if (ci.risk_level === 'Critical') {
                ownerStats[key].critical_count++;
              } else if (ci.risk_level === 'High') {
                ownerStats[key].high_count++;
              }
            }
          }
        } catch (error) {
          console.error('Error processing CI for owner stats:', ci, error);
        }
      });
      
      // Calculate average confidence and sort by total CIs
      const result = Object.values(ownerStats)
        .map(owner => ({
          ...owner,
          avg_confidence: owner.confidences.length > 0 
            ? (owner.confidences.reduce((sum, conf) => sum + conf, 0) / owner.confidences.length)
            : 0
        }))
        .sort((a, b) => b.total_cis - a.total_cis);
        
      console.log('Owner stats result:', result);
      return result;
    } catch (error) {
      console.error('Error in getRecommendedOwnersStats:', error);
      return [];
    }
  };

  // Get CIs for a specific owner
  const getCIsForOwner = (ownerUsername) => {
    try {
      if (!scanResults || !scanResults.stale_cis || !Array.isArray(scanResults.stale_cis)) {
        console.log('No scan results available for getCIsForOwner');
        return [];
      }
      
      const result = scanResults.stale_cis.filter(ci => {
        try {
          return ci && 
            (ci.risk_level === 'Critical' || ci.risk_level === 'High') &&
            ci.recommended_owners && 
            Array.isArray(ci.recommended_owners) &&
            ci.recommended_owners.length > 0 && 
            ci.recommended_owners[0] &&
            ci.recommended_owners[0].username &&
            mapSystemToAdmin(ci.recommended_owners[0].username, ci.recommended_owners[0].display_name).username === ownerUsername;
        } catch (error) {
          console.error('Error filtering CI for owner:', ci, error);
          return false;
        }
      });
      
      console.log(`Found ${result.length} CIs for owner ${ownerUsername}`);
      return result;
    } catch (error) {
      console.error('Error in getCIsForOwner:', error);
      return [];
    }
  };

  // Toggle owner expansion
  const toggleOwnerExpansion = (ownerUsername) => {
    setExpandedOwner(expandedOwner === ownerUsername ? null : ownerUsername);
  };

  // Map system user to admin user
  const mapSystemToAdmin = (username, displayName) => {
    try {
      if (!username) {
        return {
          username: 'unknown',
          display_name: 'Unknown User'
        };
      }
      
      if (username.toLowerCase() === 'system') {
        return {
          username: 'admin',
          display_name: 'System Administrator'
        };
      }
      
      return {
        username: username,
        display_name: displayName || username
      };
    } catch (error) {
      console.error('Error in mapSystemToAdmin:', error);
      return {
        username: 'unknown',
        display_name: 'Unknown User'
      };
    }
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

  const assignCIToOwner = async (ciId, ownerUsername, ownerDisplayName) => {
    if (!connectionStatus?.success) {
      alert('Please establish a successful connection first');
      return;
    }

    // Add CI to assigning set
    const newAssigning = new Set(assigningCIs);
    newAssigning.add(ciId);
    setAssigningCIs(newAssigning);

    try {
      const response = await fetch(`${config.API_URL}/assign-ci-owner`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          instance_url: formData.instanceUrl,
          username: formData.username,
          password: formData.password,
          ci_id: ciId,
          new_owner_username: ownerUsername
        }),
      });

      const result = await response.json();

      if (response.ok && result.success) {
        // Update assignment results
        setAssignmentResults(prev => ({
          ...prev,
          [ciId]: {
            success: true,
            message: result.message,
            newOwner: result.new_owner,
            timestamp: new Date().toLocaleTimeString()
          }
        }));

        // Add to assigned CIs list
        const assignedCI = scanResults.stale_cis.find(ci => ci.ci_id === ciId);
        if (assignedCI) {
          setAssignedCIs(prev => [...prev, {
            ...assignedCI,
            assigned_to: ownerDisplayName,
            assigned_username: ownerUsername,
            assignment_timestamp: new Date().toISOString(),
            assignment_message: result.message
          }]);
        }

        // Update the CI data in scanResults to reflect the new owner
        if (scanResults && scanResults.stale_cis) {
          const updatedStaleCs = scanResults.stale_cis.map(ci => {
            if (ci.ci_id === ciId) {
              return {
                ...ci,
                current_owner: ownerDisplayName
              };
            }
            return ci;
          });
          
          setScanResults(prev => ({
            ...prev,
            stale_cis: updatedStaleCs
          }));
        }
      } else {
        setAssignmentResults(prev => ({
          ...prev,
          [ciId]: {
            success: false,
            message: result.error || 'Assignment failed',
            timestamp: new Date().toLocaleTimeString()
          }
        }));
      }
    } catch (error) {
      setAssignmentResults(prev => ({
        ...prev,
        [ciId]: {
          success: false,
          message: 'Network error during assignment',
          timestamp: new Date().toLocaleTimeString()
        }
      }));
    } finally {
      // Remove CI from assigning set
      const newAssigning = new Set(assigningCIs);
      newAssigning.delete(ciId);
      setAssigningCIs(newAssigning);
    }
  };

  const fetchAssignmentHistory = async () => {
    if (!connectionStatus?.success) {
      alert('Please establish a successful connection first');
      return;
    }

    setIsLoadingHistory(true);
    try {
      const response = await fetch(`${config.API_URL}/assignment-history`);
      const result = await response.json();
      
      if (response.ok && result.success) {
        setAssignmentHistory(result.history);
      } else {
        alert(result.error || 'Failed to fetch assignment history');
      }
    } catch (error) {
      alert('Network error while fetching history');
    } finally {
      setIsLoadingHistory(false);
    }
  };

  const handleUndoAssignment = async (assignmentId) => {
    if (!connectionStatus?.success) {
      alert('Please establish a successful connection first');
      return;
    }

    if (!window.confirm('Are you sure you want to undo this assignment?')) {
      return;
    }

    setUndoingAssignment(assignmentId);
    try {
      console.log('Sending undo request for assignment:', assignmentId);
      const response = await fetch(`${config.API_URL}/undo-assignment`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          instance_url: formData.instanceUrl,
          username: formData.username,
          password: formData.password,
          assignment_id: Number(assignmentId) // Ensure it's a number
        }),
      });

      const result = await response.json();
      console.log('Undo response:', result);
      
      if (response.ok && result.success) {
        // Refresh the history
        await fetchAssignmentHistory();
        // Update the UI to reflect the change
        if (scanResults && scanResults.stale_cis) {
          const assignment = assignmentHistory.find(a => a.id === assignmentId);
          if (assignment) {
            const updatedStaleCs = scanResults.stale_cis.map(ci => {
              if (ci.ci_id === assignment.ci_id) {
                return {
                  ...ci,
                  current_owner: assignment.previous_owner.display_name
                };
              }
              return ci;
            });
            
            setScanResults(prev => ({
              ...prev,
              stale_cis: updatedStaleCs
            }));
          }
        }
      } else {
        const errorMessage = result.error || 'Failed to undo assignment';
        console.error('Undo failed:', errorMessage);
        alert(errorMessage);
      }
    } catch (error) {
      console.error('Error during undo:', error);
      alert('Network error while undoing assignment. Please check the console for details.');
    } finally {
      setUndoingAssignment(null);
    }
  };

  // Reset pagination when new results come in
  useEffect(() => {
    setCurrentPage(1);
    setExpandedCI(null);
    setExpandedAlternates(new Set());
    setActiveFilter('all');
    setAssigningCIs(new Set());
    setAssignmentResults({});
    setSearchTerm(''); // Reset search
    // Don't reset assignedCIs as they should persist across scans
  }, [scanResults]);

  // Add this to your useEffect that handles successful assignments
  useEffect(() => {
    if (assignmentResults && Object.keys(assignmentResults).length > 0) {
      // After a successful assignment, refresh the history
      fetchAssignmentHistory();
    }
  }, [assignmentResults]);

  const sortOptions = [
    { value: 'confidence', label: 'Confidence' },
    { value: 'risk', label: 'Risk Level' },
    { value: 'name', label: 'CI Name (A-Z)' },
    { value: 'date', label: 'Days Inactive' },
  ];

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
                        className="pl-10 pr-10"
                      />
                      <button
                        type="button"
                        onClick={() => setShowPassword(!showPassword)}
                        className="absolute right-3 top-3 text-gray-400 hover:text-purple-300 transition-colors"
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
                          title: "Filter by Owners",
                          value: getRecommendedOwnersStats().length,
                          icon: Users,
                          color: "text-green-400",
                          filter: "by-owner",
                          clickable: true
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



                    {/* Stale CIs List */}
                    <CardContainer>
                      <div className="overflow-hidden">
                        <div className="p-6 border-b border-white/10">
                          <div className="flex items-center justify-between">
                            <div>
                              <h3 className="text-2xl font-bold text-white flex items-center">
                                <Database className="w-6 h-6 mr-3 text-purple-400" />
                                Stale Configuration Items
                                {activeFilter === 'by-owner' && selectedOwnerFilter !== 'all' && (
                                  <span className="ml-3 px-3 py-1 bg-blue-500/20 text-blue-300 rounded-full text-sm font-medium">
                                    {getRecommendedOwnersStats().find(o => o.username === selectedOwnerFilter)?.display_name || selectedOwnerFilter}
                                  </span>
                                )}
                                {activeFilter === 'by-owner' && selectedOwnerFilter === 'all' && (
                                  <span className="ml-3 px-3 py-1 bg-purple-500/20 text-purple-300 rounded-full text-sm font-medium">
                                    High Risk & Critical Only
                                  </span>
                                )}
                                {activeFilter !== 'all' && activeFilter !== 'by-owner' && (
                                  <span className="ml-3 px-3 py-1 bg-purple-500/20 text-purple-300 rounded-full text-sm font-medium">
                                    {activeFilter.charAt(0).toUpperCase() + activeFilter.slice(1)} Risk Only
                                  </span>
                                )}
                              </h3>
                              <p className="text-gray-400 mt-2">
                                {activeFilter === 'all' 
                                  ? 'Click on any CI to view detailed analysis and recommendations' 
                                  : activeFilter === 'by-owner'
                                    ? selectedOwnerFilter === 'all'
                                      ? `Showing ${getFilteredCIs().length} high risk & critical CIs with recommendations`
                                      : `Showing ${getFilteredCIs().length} CIs recommended for ${getRecommendedOwnersStats().find(o => o.username === selectedOwnerFilter)?.display_name || selectedOwnerFilter}`
                                    : `Showing ${getFilteredCIs().length} ${activeFilter} risk CIs. Click summary cards above to change filter.`
                                }
                              </p>
                            </div>
                            
                            {activeFilter !== 'all' && (
                              <button
                                onClick={() => {
                                  setFilter('all');
                                  setSelectedOwnerFilter('all');
                                }}
                                className="px-4 py-2 bg-gradient-to-r from-purple-500/20 to-blue-500/20 hover:from-purple-500/30 hover:to-blue-500/30 border border-purple-500/30 text-purple-300 hover:text-purple-200 rounded-lg transition-all text-sm flex items-center space-x-2"
                              >
                                <span>Clear Filter</span>
                                <span className="text-xs bg-white/20 px-2 py-1 rounded">Show All</span>
                              </button>
                            )}
                          </div>
                        </div>

                        {/* Owner Selection Interface */}
                        {activeFilter === 'by-owner' && scanResults && scanResults.stale_cis && getRecommendedOwnersStats().length > 0 && (
                          <ErrorBoundary>
                          <div className="p-6 border-b border-white/10 bg-white/2">
                            <div className="mb-4">
                              <h4 className="text-lg font-semibold text-white mb-2">Select Recommended Owner</h4>
                              <p className="text-gray-400 text-sm">Choose an owner to view their high risk & critical CI recommendations</p>
                            </div>
                            
                            <div className="space-y-3">
                              {/* Individual Owners */}
                              {getRecommendedOwnersStats().map((owner) => (
                                <div key={owner.username} className="bg-white/5 rounded-lg border border-white/10 overflow-hidden">
                                  {/* Owner Header */}
                                  <div
                                    onClick={() => toggleOwnerExpansion(owner.username)}
                                    className="p-4 cursor-pointer transition-all hover:bg-white/10"
                                  >
                                    <div className="flex items-center justify-between">
                                      <div className="flex items-center space-x-3">
                                        <div className="w-8 h-8 rounded-full bg-gradient-to-r from-green-500 to-blue-500 flex items-center justify-center">
                                          <UserCheck className="w-4 h-4 text-white" />
                                        </div>
                                        <div>
                                          <div className="font-medium text-white">{owner.display_name}</div>
                                          <div className="text-xs text-gray-400">{owner.username}</div>
                                        </div>
                                      </div>
                                      <div className="flex items-center space-x-4">
                                        <div className="text-right">
                                          <div className="text-lg font-bold text-blue-400">{owner.total_cis}</div>
                                          <div className="text-xs text-gray-400">
                                            {owner.critical_count}C / {owner.high_count}H
                                          </div>
                                        </div>
                                        <div>
                                          {expandedOwner === owner.username ? 
                                            <ChevronDown className="w-5 h-5 text-gray-400" /> : 
                                            <ChevronRight className="w-5 h-5 text-gray-400" />
                                          }
                                        </div>
                                      </div>
                                    </div>
                                    <div className="mt-2 grid grid-cols-2 gap-4 text-xs">
                                      <div className="text-center">
                                        <div className="text-red-400 font-semibold">{owner.critical_count}</div>
                                        <div className="text-gray-400">Critical</div>
                                      </div>
                                      <div className="text-center">
                                        <div className="text-orange-400 font-semibold">{owner.high_count}</div>
                                        <div className="text-gray-400">High Risk</div>
                                      </div>
                                    </div>
                                  </div>

                                  {/* Expanded CIs for this owner */}
                                  {expandedOwner === owner.username && (
                                    <div className="border-t border-white/10 bg-white/2">
                                      <div className="p-4">
                                        <div className="mb-3 text-sm text-gray-400">
                                          Showing {getCIsForOwner(owner.username).length} CIs recommended for {owner.display_name}
                                        </div>
                                        <div className="space-y-3">
                                          {getCIsForOwner(owner.username).map((ci) => (
                                            <div key={ci.ci_id} className="bg-white/5 rounded-lg border border-white/10 overflow-hidden">
                                              {/* CI Header - Clickable */}
                                              <div 
                                                className="p-3 cursor-pointer transition-colors hover:bg-white/10"
                                                onClick={() => toggleCIExpansion(ci.ci_id)}
                                              >
                                                <div className="flex items-center justify-between mb-2">
                                                  <div className="flex items-center space-x-3">
                                                    <div className="flex items-center">
                                                      {expandedCI === ci.ci_id ? 
                                                        <ChevronDown className="w-4 h-4 text-gray-400" /> : 
                                                        <ChevronRight className="w-4 h-4 text-gray-400" />
                                                      }
                                                    </div>
                                                    <div className="font-medium text-white text-sm">{ci.ci_name}</div>
                                                    <span className={`px-2 py-1 text-xs rounded ${getRiskColor(ci.risk_level)}`}>
                                                      {ci.risk_level}
                                                    </span>
                                                  </div>
                                                  <div className="text-right">
                                                    <div className="text-green-400 font-semibold text-sm">{(ci.confidence * 100).toFixed(0)}%</div>
                                                    <div className="text-xs text-gray-400">Confidence</div>
                                                  </div>
                                                </div>
                                                
                                                <div className="text-xs text-gray-400">
                                                  <span className="font-medium">Class:</span> {ci.ci_class} • 
                                                  <span className="font-medium ml-2">Current Owner:</span> {ci.current_owner}
                                                </div>
                                              </div>

                                              {/* Expanded Details */}
                                              {expandedCI === ci.ci_id && (
                                                <div className="bg-white/2 p-4 border-t border-white/10">
                                                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">
                                                    {/* Staleness Reasons */}
                                                    <div>
                                                      <h6 className="text-white font-semibold mb-2 flex items-center text-sm">
                                                        <AlertTriangle className="w-4 h-4 mr-2 text-orange-400" />
                                                        Why This CI is Stale
                                                      </h6>
                                                      <div className="space-y-2">
                                                        {ci.staleness_reasons && ci.staleness_reasons.map((reason, reasonIdx) => (
                                                          <div key={reasonIdx} className="bg-white/5 p-2 rounded-lg border border-white/10">
                                                            <div className="flex items-center justify-between mb-1">
                                                              <span className="text-purple-300 font-medium text-xs">
                                                                {reason.rule_name && reason.rule_name.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                                                              </span>
                                                              <span className="text-xs text-gray-400">
                                                                {reason.confidence && (reason.confidence * 100).toFixed(0)}% confidence
                                                              </span>
                                                            </div>
                                                            <p className="text-gray-300 text-xs">{reason.description}</p>
                                                          </div>
                                                        ))}
                                                      </div>
                                                    </div>

                                                    {/* Recommended Owners */}
                                                    <div>
                                                      <h6 className="text-white font-semibold mb-2 flex items-center text-sm">
                                                        <Users className="w-4 h-4 mr-2 text-green-400" />
                                                        Recommended New Owners
                                                      </h6>
                                                      <div className="space-y-2">
                                                        {ci.recommended_owners && ci.recommended_owners.slice(0, 2).map((rec, recIdx) => (
                                                          <div key={recIdx} className="bg-white/5 p-2 rounded-lg border border-white/10">
                                                            <div className="flex items-center justify-between mb-1">
                                                              <span className="text-green-300 font-medium text-xs">
                                                                {rec.display_name || rec.username}
                                                              </span>
                                                              <span className="text-green-400 font-semibold text-xs">
                                                                {(rec.confidence * 100).toFixed(0)}%
                                                              </span>
                                                            </div>
                                                            <div className="grid grid-cols-2 gap-2 text-xs text-gray-300">
                                                              <div>
                                                                <span className="text-gray-400">Activities: </span>
                                                                <span className="text-green-300">{rec.activity_count || 0}</span>
                                                              </div>
                                                              <div>
                                                                <span className="text-gray-400">Last Seen: </span>
                                                                <span className="text-green-300">
                                                                  {rec.days_since_activity === 999 ? 'Never' : `${rec.days_since_activity || 0}d ago`}
                                                                </span>
                                                              </div>
                                                            </div>
                                                            {rec.reason && (
                                                              <div className="text-xs text-green-200 mt-1">
                                                                <span className="text-gray-400">Why: </span>
                                                                <span>{rec.reason}</span>
                                                              </div>
                                                            )}
                                                          </div>
                                                        ))}
                                                      </div>
                                                    </div>
                                                  </div>
                                                  
                                                  {/* Assign Button */}
                                                  <div className="pt-3 border-t border-white/10">
                                                    {assignmentResults[ci.ci_id] ? (
                                                      <div className={`text-center p-2 rounded-lg ${
                                                        assignmentResults[ci.ci_id].success 
                                                          ? 'bg-green-500/20 text-green-400' 
                                                          : 'bg-red-500/20 text-red-400'
                                                      }`}>
                                                        <div className="flex items-center justify-center space-x-2">
                                                          {assignmentResults[ci.ci_id].success ? (
                                                            <CheckCircle2 className="w-4 h-4" />
                                                          ) : (
                                                            <AlertCircle className="w-4 h-4" />
                                                          )}
                                                          <span className="text-sm font-medium">
                                                            {assignmentResults[ci.ci_id].message}
                                                          </span>
                                                        </div>
                                                        <div className="text-xs mt-1 opacity-75">
                                                          {assignmentResults[ci.ci_id].timestamp}
                                                        </div>
                                                      </div>
                                                    ) : (
                                                      <button
                                                        onClick={(e) => {
                                                          e.stopPropagation();
                                                          const mappedOwner = mapSystemToAdmin(owner.username, owner.display_name);
                                                          assignCIToOwner(ci.ci_id, mappedOwner.username, mappedOwner.display_name);
                                                        }}
                                                        disabled={assigningCIs.has(ci.ci_id)}
                                                        className={`w-full flex items-center justify-center space-x-2 py-2 px-4 rounded-lg font-medium transition-all text-sm ${
                                                          assigningCIs.has(ci.ci_id)
                                                            ? 'bg-gray-600 text-gray-400 cursor-not-allowed'
                                                            : 'bg-gradient-to-r from-purple-500 to-blue-600 hover:from-purple-600 hover:to-blue-700 text-white hover:scale-105'
                                                        }`}
                                                      >
                                                        {assigningCIs.has(ci.ci_id) ? (
                                                          <>
                                                            <Loader2 className="w-4 h-4 animate-spin" />
                                                            <span>Assigning...</span>
                                                          </>
                                                        ) : (
                                                          <>
                                                            <UserPlus className="w-4 h-4" />
                                                            <span>Assign to {mapSystemToAdmin(owner.username, owner.display_name).display_name}</span>
                                                          </>
                                                        )}
                                                      </button>
                                                    )}
                                                  </div>
                                                </div>
                                              )}
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    </div>
                                  )}
                                </div>
                              ))}
                            </div>
                          </div>
                          </ErrorBoundary>
                        )}

                        {/* Search and Sort Controls */}
                        {scanResults && scanResults.stale_cis && scanResults.stale_cis.length > 0 && activeFilter !== 'by-owner' && (
                          <div className="p-6 border-b border-white/10 bg-white/2">
                            <div className="flex flex-col md:flex-row gap-4 items-center justify-between">
                              {/* Search Bar */}
                              <div className="relative flex-1 max-w-md">
                                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
                                <input
                                  type="text"
                                  placeholder="Search CI by name"
                                  value={searchTerm}
                                  onChange={(e) => setSearchTerm(e.target.value)}
                                  className="w-full pl-10 pr-4 py-2 bg-white/10 border border-white/20 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                                />
                                {searchTerm && (
                                  <button
                                    onClick={() => setSearchTerm('')}
                                    className="absolute right-3 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-purple-300 transition-colors"
                                  >
                                    <X className="w-4 h-4" />
                                  </button>
                                )}
                              </div>

                              {/* Sort Controls */}
                              <div className="flex items-center space-x-3">
                                <span className="text-gray-400 text-sm">Sort by:</span>
                                <Listbox value={sortBy} onChange={setSortBy}>
                                  <div className="relative">
                                    <Listbox.Button className="bg-white/10 border border-white/20 rounded-lg px-3 py-2 text-gray-200 text-sm w-48 text-left">
                                      {sortOptions.find(opt => opt.value === sortBy)?.label}
                                    </Listbox.Button>
                                    <Listbox.Options className="absolute mt-1 w-48 bg-[#23232b] border border-white/20 rounded-lg shadow-lg z-10">
                                      {sortOptions.map(option => (
                                        <Listbox.Option
                                          key={option.value}
                                          value={option.value}
                                          className={({ active }) =>
                                            `cursor-pointer select-none px-4 py-2 text-gray-200 ${
                                              active ? 'bg-white/20' : ''
                                            }`
                                          }
                                        >
                                          {option.label}
                                        </Listbox.Option>
                                      ))}
                                    </Listbox.Options>
                                  </div>
                                </Listbox>
                                
                                <button
                                  onClick={() => setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc')}
                                  className="px-3 py-2 bg-gradient-to-r from-purple-500/20 to-blue-500/20 hover:from-purple-500/30 hover:to-blue-500/30 border border-purple-500/30 text-purple-300 hover:text-purple-200 rounded-lg transition-colors"
                                  title={`Sort ${sortOrder === 'asc' ? 'Descending' : 'Ascending'}`}
                                >
                                  {sortOrder === 'asc' ? (
                                    <ArrowUp className="w-4 h-4" />
                                  ) : (
                                    <ArrowDown className="w-4 h-4" />
                                  )}
                                </button>

                                <div className="flex items-center space-x-2">
                                  <button
                                    onClick={() => {
                                      setShowAssignmentHistory(true);
                                      fetchAssignmentHistory();
                                    }}
                                    className="px-4 py-2 bg-gradient-to-r from-purple-500/20 to-blue-500/20 hover:from-purple-500/30 hover:to-blue-500/30 border border-purple-500/30 text-purple-300 hover:text-purple-200 rounded-lg transition-all text-sm flex items-center space-x-2"
                                  >
                                    <History className="w-4 h-4" />
                                    <span>Assignment History</span>
                                  </button>
                                </div>
                              </div>
                            </div>

                            {/* Search Results Info */}
                            {searchTerm && (
                              <div className="mt-3 text-sm text-gray-400">
                                Found {getFilteredCIs().length} CIs matching "{searchTerm}"
                                {getFilteredCIs().length !== scanResults.stale_cis.length && (
                                  <span className="ml-2">
                                    (from {scanResults.stale_cis.length} total)
                                  </span>
                                )}
                              </div>
                            )}
                          </div>
                        )}
                        
                        {/* Show CI List only when not in owner selection mode */}
                        {scanResults && scanResults.stale_cis && scanResults.stale_cis.length > 0 && activeFilter !== 'by-owner' ? (
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
                                              
                                              {/* Assign Button */}
                                              <div className="mt-3 pt-3 border-t border-white/10">
                                                {assignmentResults[ci.ci_id] ? (
                                                  <div className={`text-center p-2 rounded-lg ${
                                                    assignmentResults[ci.ci_id].success 
                                                      ? 'bg-green-500/20 text-green-400' 
                                                      : 'bg-red-500/20 text-red-400'
                                                  }`}>
                                                    <div className="flex items-center justify-center space-x-2">
                                                      {assignmentResults[ci.ci_id].success ? (
                                                        <CheckCircle2 className="w-4 h-4" />
                                                      ) : (
                                                        <AlertCircle className="w-4 h-4" />
                                                      )}
                                                      <span className="text-sm font-medium">
                                                        {assignmentResults[ci.ci_id].message}
                                                      </span>
                                                    </div>
                                                    <div className="text-xs mt-1 opacity-75">
                                                      {assignmentResults[ci.ci_id].timestamp}
                                                    </div>
                                                  </div>
                                                ) : (
                                                  <button
                                                    onClick={() => {
                                                      const mappedOwner = mapSystemToAdmin(
                                                        ci.recommended_owners[0].username, 
                                                        ci.recommended_owners[0].display_name
                                                      );
                                                      assignCIToOwner(ci.ci_id, mappedOwner.username, mappedOwner.display_name);
                                                    }}
                                                    disabled={assigningCIs.has(ci.ci_id)}
                                                    className={`w-full flex items-center justify-center space-x-2 py-2 px-4 rounded-lg font-medium transition-all ${
                                                      assigningCIs.has(ci.ci_id)
                                                        ? 'bg-gray-600 text-gray-400 cursor-not-allowed'
                                                        : 'bg-gradient-to-r from-purple-500 to-blue-600 hover:from-purple-600 hover:to-blue-700 text-white hover:scale-105'
                                                    }`}
                                                  >
                                                    {assigningCIs.has(ci.ci_id) ? (
                                                      <>
                                                        <Loader2 className="w-4 h-4 animate-spin" />
                                                        <span>Assigning...</span>
                                                      </>
                                                    ) : (
                                                      <>
                                                        <UserPlus className="w-4 h-4" />
                                                        <span>Assign to {mapSystemToAdmin(ci.recommended_owners[0].username, ci.recommended_owners[0].display_name).display_name}</span>
                                                      </>
                                                    )}
                                                  </button>
                                                )}
                                              </div>
                                            </div>

                                            {/* Show Alternate Recommendations Button */}
                                            {ci.recommended_owners.length >= 1 && (
                                              <div className="text-center">
                                                <button
                                                  onClick={() => toggleAlternateRecommendations(ci.ci_id)}
                                                  className="flex items-center space-x-2 mx-auto px-4 py-2 bg-gradient-to-r from-purple-500/20 to-blue-500/20 hover:from-purple-500/30 hover:to-blue-500/30 border border-purple-500/30 text-purple-300 hover:text-purple-200 rounded-lg transition-all text-sm"
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
                                                    
                                                    {/* Assign Button for Alternate */}
                                                    <div className="mt-3 pt-3 border-t border-white/10">
                                                      <button
                                                        onClick={() => {
                                                          const mappedOwner = mapSystemToAdmin(owner.username, owner.display_name);
                                                          assignCIToOwner(ci.ci_id, mappedOwner.username, mappedOwner.display_name);
                                                        }}
                                                        disabled={assigningCIs.has(ci.ci_id)}
                                                        className={`w-full flex items-center justify-center space-x-2 py-2 px-3 rounded-lg font-medium transition-all text-sm ${
                                                          assigningCIs.has(ci.ci_id)
                                                            ? 'bg-gray-600 text-gray-400 cursor-not-allowed'
                                                            : 'bg-gradient-to-r from-purple-500 to-blue-600 hover:from-purple-600 hover:to-blue-700 text-white hover:scale-105'
                                                        }`}
                                                      >
                                                        {assigningCIs.has(ci.ci_id) ? (
                                                          <>
                                                            <Loader2 className="w-3 h-3 animate-spin" />
                                                            <span>Assigning...</span>
                                                          </>
                                                        ) : (
                                                          <>
                                                            <UserPlus className="w-3 h-3" />
                                                            <span>Assign</span>
                                                          </>
                                                        )}
                                                      </button>
                                                    </div>
                                                  </div>
                                                ))}

                                                {/* Show current owner as alternate when only 1 recommendation */}
                                                {ci.recommended_owners.length === 1 && (
                                                  <div className="bg-white/3 p-3 rounded-lg border border-white/5">
                                                    <div className="flex items-center justify-between mb-2">
                                                      <div>
                                                        <div className="text-orange-300 font-medium">{ci.current_owner} (Current)</div>
                                                        <div className="text-xs text-gray-400">{ci.current_owner_username || ci.current_owner}</div>
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
                        ) : activeFilter !== 'by-owner' && (
                          <div className="p-8 text-center">
                            <CheckCircle className="w-12 h-12 text-green-400 mx-auto mb-4" />
                            <p className="text-green-400 text-lg font-medium">No stale ownership detected!</p>
                            <p className="text-gray-400 text-sm">All CIs appear to have appropriate ownership assignments.</p>
                          </div>
                        )}

                        {/* Pagination Controls */}
                        {getFilteredCIs().length > itemsPerPage && activeFilter !== 'by-owner' && (
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
                                      : 'bg-gradient-to-r from-purple-500/20 to-blue-500/20 hover:from-purple-500/30 hover:to-blue-500/30 border border-purple-500/30 text-purple-300 hover:text-purple-200 hover:scale-105'
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
                                            ? 'bg-gradient-to-r from-purple-500 to-blue-600 text-white shadow-lg'
                                            : 'bg-white/10 text-gray-300 hover:bg-gradient-to-r hover:from-purple-500/20 hover:to-blue-500/20 hover:text-purple-300 border hover:border-purple-500/30'
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
                                        className="px-3 py-2 rounded-lg text-sm font-medium bg-white/10 text-gray-300 hover:bg-gradient-to-r hover:from-purple-500/20 hover:to-blue-500/20 hover:text-purple-300 border hover:border-purple-500/30 transition-all"
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
                                      : 'bg-gradient-to-r from-purple-500/20 to-blue-500/20 hover:from-purple-500/30 hover:to-blue-500/30 border border-purple-500/30 text-purple-300 hover:text-purple-200 hover:scale-105'
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




                  </>
                )}
              </div>
            )}
          </div>


        </div>
      </div>

      {/* Assignment History Modal */}
      {showAssignmentHistory && (
        <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50">
          <div className="bg-gray-900 rounded-xl border border-white/10 w-full max-w-4xl max-h-[80vh] overflow-hidden">
            <div className="p-6 border-b border-white/10 flex items-center justify-between">
              <div>
                <h3 className="text-2xl font-bold text-white flex items-center">
                  <History className="w-6 h-6 mr-3 text-purple-400" />
                  CI Assignment History
                </h3>
                <p className="text-gray-400 mt-2">Track and manage CI ownership changes</p>
              </div>
              <button
                onClick={() => setShowAssignmentHistory(false)}
                className="text-gray-400 hover:text-purple-300 transition-colors"
              >
                <X className="w-6 h-6" />
              </button>
            </div>

            <div className="p-6 overflow-y-auto max-h-[calc(80vh-140px)]">
              {isLoadingHistory ? (
                <div className="flex items-center justify-center py-12">
                  <Loader2 className="w-8 h-8 animate-spin text-purple-400" />
                  <span className="ml-3 text-gray-400">Loading assignment history...</span>
                </div>
              ) : assignmentHistory.length === 0 ? (
                <div className="text-center py-12">
                  <History className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                  <p className="text-gray-400">No assignment history found</p>
                  <p className="text-sm text-gray-500 mt-2">Assignment history will appear here after CIs are assigned to new owners</p>
                </div>
              ) : (
                <div className="space-y-4">
                  {assignmentHistory.map((assignment) => (
                    <div
                      key={assignment.id}
                      className={`bg-white/5 rounded-lg border ${
                        assignment.is_undo ? 'border-blue-500/30' : 'border-white/10'
                      } p-4`}
                    >
                      <div className="flex items-center justify-between mb-3">
                        <div className="flex items-center space-x-3">
                          {assignment.is_undo ? (
                            <div className="w-10 h-10 rounded-lg bg-blue-500/20 flex items-center justify-center">
                              <RotateCcw className="w-5 h-5 text-blue-400" />
                            </div>
                          ) : (
                            <div className="w-10 h-10 rounded-lg bg-purple-500/20 flex items-center justify-center">
                              <UserPlus className="w-5 h-5 text-purple-400" />
                            </div>
                          )}
                          <div>
                            <h4 className="text-white font-medium">{assignment.ci_name}</h4>
                            <p className="text-sm text-gray-400">{assignment.ci_class}</p>
                          </div>
                        </div>
                        <div className="text-right">
                          <div className="text-sm text-gray-400">
                            {new Date(assignment.timestamp).toLocaleString()}
                          </div>
                          {!assignment.is_undo && (
                            <button
                              onClick={() => handleUndoAssignment(assignment.id)}
                              disabled={undoingAssignment === assignment.id}
                              className={`mt-2 px-3 py-1 rounded text-xs font-medium flex items-center space-x-1 ${
                                undoingAssignment === assignment.id
                                  ? 'bg-gray-700 text-gray-400 cursor-not-allowed'
                                  : 'bg-gradient-to-r from-purple-500/20 to-blue-500/20 text-purple-300 hover:from-purple-500/30 hover:to-blue-500/30 border border-purple-500/30'
                              }`}
                            >
                              {undoingAssignment === assignment.id ? (
                                <>
                                  <Loader2 className="w-3 h-3 animate-spin" />
                                  <span>Undoing...</span>
                                </>
                              ) : (
                                <>
                                  <RotateCcw className="w-3 h-3" />
                                  <span>Undo</span>
                                </>
                              )}
                            </button>
                          )}
                        </div>
                      </div>

                      <div className="grid grid-cols-2 gap-4 text-sm">
                        <div className="bg-black/20 rounded p-3">
                          <div className="text-gray-400 mb-1">Previous Owner</div>
                          <div className="text-white">
                            {assignment.previous_owner?.display_name || 'Unknown'}
                            {assignment.previous_owner?.username && (
                              <span className="text-xs text-gray-400 ml-2">
                                ({assignment.previous_owner.username})
                              </span>
                            )}
                          </div>
                        </div>
                        <div className="bg-black/20 rounded p-3">
                          <div className="text-gray-400 mb-1">New Owner</div>
                          <div className="text-white">
                            {assignment.new_owner?.display_name || 'Unknown'}
                            {assignment.new_owner?.username && (
                              <span className="text-xs text-gray-400 ml-2">
                                ({assignment.new_owner.username})
                              </span>
                            )}
                          </div>
                        </div>
                      </div>

                      {assignment.is_undo && (
                        <div className="mt-3 text-xs text-blue-400 bg-blue-500/10 rounded p-2">
                          This is an undo operation of a previous assignment
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>

            <div className="p-6 border-t border-white/10 bg-white/2">
              <div className="flex justify-between items-center">
                <div className="text-sm text-gray-400">
                  Total: {assignmentHistory.length} assignment records
                </div>
                <button
                  onClick={() => setShowAssignmentHistory(false)}
                  className="px-4 py-2 bg-gradient-to-r from-purple-500/20 to-blue-500/20 hover:from-purple-500/30 hover:to-blue-500/30 border border-purple-500/30 text-purple-300 hover:text-purple-200 rounded-lg transition-all text-sm"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Scroll Navigation Buttons */}
      {showScrollButtons && (
        <div className="fixed bottom-6 right-6 flex flex-col space-y-3 z-40">
          {/* Scroll to Top Button */}
          <button
            onClick={scrollToTop}
            className="group w-12 h-12 bg-gradient-to-r from-purple-500 to-blue-600 hover:from-purple-600 hover:to-blue-700 rounded-full shadow-lg hover:shadow-xl transition-all duration-300 flex items-center justify-center hover:scale-110"
            title="Scroll to Top"
          >
            <ChevronUp className="w-5 h-5 text-white group-hover:scale-110 transition-transform" />
          </button>
          
          {/* Scroll to Bottom Button */}
          <button
            onClick={scrollToBottom}
            className="group w-12 h-12 bg-gradient-to-r from-blue-500 to-cyan-600 hover:from-blue-600 hover:to-cyan-700 rounded-full shadow-lg hover:shadow-xl transition-all duration-300 flex items-center justify-center hover:scale-110"
            title="Scroll to Bottom"
          >
            <ChevronDown className="w-5 h-5 text-white group-hover:scale-110 transition-transform" />
          </button>
        </div>
      )}
    </div>
  );
};

export default ServiceNowScanner;