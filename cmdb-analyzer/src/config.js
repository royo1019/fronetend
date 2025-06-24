// Configuration for different environments
const config = {
  development: {
    API_URL: 'http://localhost:5000'
  },
  production: {
    // Replace this with your deployed backend URL
    API_URL: 'https://your-backend-service.herokuapp.com' // or Railway, Render, etc.
  }
};

const environment = import.meta.env.MODE || 'development';

export default config[environment]; 