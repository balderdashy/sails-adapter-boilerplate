![image_squidhome@2x.png](http://i.imgur.com/RIvu9.png) 

# YelpAdapter

This adapter extends the node-yelp module to Sails.js. (https://github.com/olalonde/node-yelp).

## Installation

This isn't released as an npm module so you have to download YelpAdapter.js and place it in your `api/adapters` directory.

## Setup

Add your yelp credentials to config/application.yml

```
development:
  yelp:
    consumer_key: "your_consumer_key"
    consumer_secret: "your_consumer_secret"
    token: "your_token"
    token_secret: "your_token_secret"
```

Then require them in config/application.js
```
require('js-yaml');
global.NODE_ENV = process.env.ENV || 'development'
global.appConfig = require('./local.yml')[NODE_ENV]
```

## Usage

Create a YelpBusiness model hooked up to the yelp adapter:

// api/models/YelpBusiness.js

```
module.exports = {
	adapter: 'yelp'
};
```

Then you can use it:
```
YelpBusiness.business("yelp-san-francisco", function(error, data) {
  console.log(error);
  console.log(data);
});

YelpBusiness.search("Tacos", "San Francisco, CA", function(error, data) {
  console.log(error);
  console.log(data);
});
```

## About Sails.js and Waterline
http://SailsJs.com

Waterline is a new kind of storage and retrieval engine for Sails.js.  It provides a uniform API for accessing stuff from different kinds of databases, protocols, and 3rd party APIs.  That means you write the same code to get users, whether they live in mySQL, LDAP, MongoDB, or Facebook.
