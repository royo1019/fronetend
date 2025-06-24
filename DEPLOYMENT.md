# ServiceNow CMDB Analyzer - Deployment Guide

This guide will help you deploy the ServiceNow CMDB Analyzer application with the React frontend on GitHub Pages and the Python Flask backend on a cloud service.

## Architecture

- **Frontend**: React app deployed to GitHub Pages
- **Backend**: Python Flask API deployed to cloud service (Heroku, Railway, Render, etc.)

## Step 1: Deploy the Backend

### Option A: Deploy to Railway (Recommended)

1. Go to [Railway.app](https://railway.app) and sign up
2. Click "New Project" → "Deploy from GitHub repo"
3. Select your repository
4. Railway will auto-detect the Python app in the `backend` folder
5. Add environment variables if needed
6. Deploy and get your backend URL (e.g., `https://your-app.railway.app`)

### Option B: Deploy to Render

1. Go to [Render.com](https://render.com) and sign up
2. Click "New" → "Web Service"
3. Connect your GitHub repository
4. Set:
   - **Root Directory**: `backend`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python app.py`
5. Deploy and get your backend URL

### Option C: Deploy to Heroku

1. Install Heroku CLI
2. Create a `Procfile` in the backend folder:
   ```
   web: python app.py
   ```
3. Create `requirements.txt` in the backend folder:
   ```bash
   cd backend
   pip freeze > requirements.txt
   ```
4. Deploy:
   ```bash
   heroku create your-app-name
   git subtree push --prefix backend heroku main
   ```

## Step 2: Update Frontend Configuration

1. Edit `cmdb-analyzer/src/config.js`
2. Replace the production API_URL with your deployed backend URL:
   ```javascript
   production: {
     API_URL: 'https://your-backend-service.railway.app' // Your actual backend URL
   }
   ```

## Step 3: Deploy Frontend to GitHub Pages

### Automatic Deployment (Recommended)

1. Push your changes to GitHub:
   ```bash
   git add .
   git commit -m "Setup deployment configuration"
   git push origin main
   ```

2. Enable GitHub Pages:
   - Go to your GitHub repository
   - Click "Settings" → "Pages"
   - Under "Source", select "GitHub Actions"

3. The GitHub Action will automatically build and deploy your React app

### Manual Deployment

1. Install dependencies and build:
   ```bash
   cd cmdb-analyzer
   npm install
   npm run build
   ```

2. Deploy to GitHub Pages:
   ```bash
   npm run deploy
   ```

## Step 4: Access Your Application

- **Frontend**: `https://yourusername.github.io/fronetend/`
- **Backend**: Your cloud service URL

## Environment Variables

Make sure your backend deployment includes:
- Any ServiceNow connection settings
- CORS settings for your frontend domain
- Any other required environment variables

## Troubleshooting

### CORS Issues
If you get CORS errors, update your Flask backend to allow your GitHub Pages domain:

```python
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins=['https://yourusername.github.io'])
```

### Build Issues
- Make sure Node.js version is compatible (18+)
- Check that all dependencies are installed
- Verify the base path in `vite.config.js` matches your repository name

### Backend Connection Issues
- Verify the backend URL in `config.js` is correct
- Check that the backend service is running
- Test API endpoints directly in browser/Postman

## Files Created for Deployment

- `.github/workflows/deploy.yml` - GitHub Actions workflow
- `cmdb-analyzer/src/config.js` - Environment configuration
- Updated `vite.config.js` - Build configuration
- Updated `package.json` - Deployment scripts

## Next Steps

1. Deploy your backend to a cloud service
2. Update the production API URL in `config.js`
3. Push to GitHub to trigger automatic deployment
4. Test your deployed application

Your ServiceNow CMDB Analyzer will be live and accessible via GitHub Pages! 