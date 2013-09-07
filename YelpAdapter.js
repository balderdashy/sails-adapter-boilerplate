/*---------------------------------------------------------------
  :: sails-yelp
  -> adapter
---------------------------------------------------------------*/
    
// Module dependencies and credentials assignment
var yelp = require('yelp').createClient({
  consumer_key: appConfig.yelp.consumer_key || '',
  consumer_secret: appConfig.yelp.consumer_secret || '',
  token: appConfig.yelp.token || '',
  token_secret: appConfig.yelp.token_secret || ''
})

/*---------------------------------------------------------------
// Set up global.appConfig variable for yelp api keys
// in config/local.yml with format such as:

development:
  yelp:
    consumer_key: "your_consumer_key"
    consumer_secret: "your_consumer_secret"
    token: "your_token"
    token_secret: "your_token_secret"

// config/application.js....

// require('js-yaml');
// global.NODE_ENV = process.env.ENV || 'development'
// global.appConfig = require('./local.yml')[NODE_ENV]

---------------------------------------------------------------*/


// Define the adapter
var adapter = {
  
  // Search the Yelp Search API for a search term and/or location
  // e.g. "term: tacos, location: San Francisco, CA"
  // See http://www.yelp.com/developers/documentation/v2/search_api
  search: function(collectionName, term, location, callback) {
    
    // If location is not a function transpose location and callback
    if (location && typeof(location) === "function") callback = location
    
    // If term is not a function transpose term and callback
    if (term && typeof(term) === "function") callback = term
    
    // Error states for not providing term or location is provided
    // by the state when location is undefined as both are overwritten
    var err = [{ err: { message: "Must provide a search term or a location."}}]
    if (typeof(location) === "undefined") return callback(err)
    
    // Search, return errors and data from search
    yelp.search({term: term, location: location}, function(error, data) {
      
      //Callback is present return errors and data
      if (callback && typeof(callback) === "function") return callback(error, data);
            
    });
  
  },
  
  // Search the Yelp Business API for a specific business name
  // e.g. ('Fred's tacos')
  // See http://www.yelp.com/developers/documentation/v2/business
  business: function(collectionName, term, callback) {
    
    // If term is a function then a business search term name 
    // wasn't supplied and respond back with an error
    if (term && typeof(term) === "function")  {
      var err = [{ err: { message: "Must provide a business yelp ID."}}]
      return callback(err)  
    }
    
    // Otherwise, search for the business name through the business API    
    yelp.business(term, function(error, data) {
      
      //Callback is present return errors and data
      if (callback && typeof(callback) === "function") return callback(error, data);
 
    });
    
  }

};

module.exports = adapter;
