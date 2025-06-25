// Configuration for different environments
const config = {
  development: {
    API_URL: 'http://cmdb-backend-final.eba-pp2diqpq.us-east-1.elasticbeanstalk.com'
  },
  production: {
    // Check if we're on Netlify (or any HTTPS site)
    API_URL: typeof window !== 'undefined' && window.location.protocol === 'https:' 
      ? '/api'  // Use proxy for HTTPS sites
      : 'http://cmdb-backend-final.eba-pp2diqpq.us-east-1.elasticbeanstalk.com'
  }
};

const environment = import.meta.env.MODE || 'development';

export default config[environment];