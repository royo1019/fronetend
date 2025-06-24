// Configuration for different environments
const config = {
  development: {
    API_URL: 'http://cmdb-backend-final.eba-pp2diqpq.us-east-1.elasticbeanstalk.com'
  },
  production: {
    // Replace this with your deployed backend URL
    API_URL: 'http://cmdb-backend-final.eba-pp2diqpq.us-east-1.elasticbeanstalk.com' // or Railway, Render, etc.
  }
};

const environment = import.meta.env.MODE || 'development';

export default config[environment]; 