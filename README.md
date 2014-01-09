![image_squidhome@2x.png](http://i.imgur.com/RIvu9.png) 

# BoilerplateAdapter

This template exists to make it easier for you to get started writing an adapter for Sails.js.

> ### WARNING
>
> This version of the adapter is for the upcoming v0.10 release of Sails / Waterline.
> Check out the 0.8 branch for the original stuff.


## Getting started
It's usually pretty easy to add your own adapters for integrating with proprietary systems or existing open APIs.  For most things, it's as easy as `require('some-module')` and mapping the appropriate methods to match waterline semantics.  To get started:

1. Fork this repository
2. Set up your README and package.json file.  Sails.js adapter module names are of the form sails-*, where * is the name of the datastore or service you're integrating with.
3. Build your adapter.

## How to test your adapter
1. Run `npm link` in this adapter's directory
2. Clone the sails.js core and modify the tests to use your new adapter.
3. Run `npm link sails-boilerplate`
4. From the sails.js core directory, run `npm test`.

## Publish your adapter

> You're welcome to write proprietary adapters and use them any way you wish--
> these instructions are for releasing an open-source adapter.

1. Do a pull request to this repository (make sure you attribute yourself as the author set the license in the package.json to "MIT")  Please let us know about any special instructions for usage/testing. 
2. Run the tests one last time.
3. We'll update the documentation with information about your new adapter
4. Then everyone will adore you with lavish praises.
5. Mike might even send you jelly beans.


## About Sails.js and Waterline
http://sailsjs.org

Waterline is a new kind of storage and retrieval engine for Sails.js.  It provides a uniform API for accessing stuff from different kinds of databases, protocols, and 3rd party APIs.  That means you write the same code to get users, whether they live in mySQL, LDAP, MongoDB, or Facebook.

