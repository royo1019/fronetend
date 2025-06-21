# CMDB Analyzer Flask Backend

This Flask backend provides ServiceNow integration for the CMDB Stale Ownership Analyzer.

## Features

- **ServiceNow Connection Testing**: Validates credentials and connectivity
- **Real API Integration**: Makes actual calls to ServiceNow REST APIs
- **Error Handling**: Comprehensive error handling for different failure scenarios
- **CORS Support**: Configured for frontend communication

## Setup Instructions

### 1. Install Python Dependencies

```bash
cd backend
pip install -r requirements.txt
```

### 2. Run the Flask Server

```bash
python app.py
```

The server will start on `http://localhost:5000`

### 3. Test the Connection

The backend provides the following endpoints:

- `POST /api/servicenow/test-connection` - Test ServiceNow connection
- `POST /api/servicenow/scan-stale-ownership` - Scan for stale ownership (placeholder)
- `GET /health` - Health check endpoint

## API Usage

### Test Connection Endpoint

**POST** `/api/servicenow/test-connection`

**Request Body:**
```json
{
  "instanceUrl": "https://your-instance.service-now.com",
  "username": "your_username",
  "password": "your_password"
}
```

**Success Response:**
```json
{
  "success": true,
  "message": "Connection successful! Connected to https://your-instance.service-now.com",
  "details": {
    "instance_url": "https://your-instance.service-now.com",
    "authenticated_user": "your_username",
    "test_query_results": 1
  }
}
```

**Error Response:**
```json
{
  "success": false,
  "message": "Authentication failed. Please check your username and password."
}
```

## How It Works

1. **URL Validation**: Validates and formats the ServiceNow instance URL
2. **Authentication**: Creates Basic Auth header with provided credentials
3. **Test Query**: Makes a limited API call to `sys_user` table to test connectivity
4. **Response Handling**: Returns detailed success/error information

## Security Features

- Input validation and sanitization
- Secure credential handling
- Timeout protection (10 seconds)
- SSL certificate verification
- Minimal data exposure in test queries

## Error Handling

The backend handles various error scenarios:

- **401 Unauthorized**: Invalid credentials
- **403 Forbidden**: Insufficient permissions
- **408 Timeout**: Connection timeout
- **503 Service Unavailable**: Cannot connect to instance
- **500 Internal Server Error**: Unexpected errors

## Next Steps

- Implement actual CMDB analysis functionality
- Add authentication/session management
- Implement ML model integration for stale ownership detection
- Add logging and monitoring 