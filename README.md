![image_squidhome@2x.png](http://i.imgur.com/RIvu9.png)

# SQLite3 Sails/Waterline Adapter

A [Waterline](https://github.com/balderdashy/waterline) adapter for SQLite3. May be used in a [Sails](https://github.com/balderdashy/sails) app or anything using Waterline for the ORM.


## Disclaimers
- SQLite3 adapter is not optimized for performance. Native joins are not implemented (among other issues).
- This codebase contains no unit tests, though all integration tests with the waterline api (v1) pass.

#### People who should use this package right now:
Those prototyping apps with sailsjs and looking to use sqlite for a test database.

For anyone looking to use this adapter in production, contributions welcome!

## Getting started
To use this in your sails app, install using:

> npm install --save sails-sqlite3

In your `config\datastores.js` file, add a property with your datastore name. Supported configuration:

```js
default: {
  adapter: 'sails-sqlite3',
  filename: '[YOUR DATABASE].db',
  mode: AS PER sqlite3 MODE OPTIONS,
  verbose: false
}
```

For more information on the `mode` configuration property, see the [driver documentation](https://github.com/mapbox/node-sqlite3/wiki/API#new-sqlite3databasefilename-mode-callback)

## Testing

> npm test

Currently only `waterline-adapter-tests` are hooked up. Passing interfaces:

- semantic
- queryable
- associations
- migratable

## Acknowledgements
This is a rewrite from a fork of the sails-sqlite3 adapter written for sailsjs < 1.0.0 originally by [Andrew Jo](https://github.com/AndrewJo). I borrowed most of the structure of the code and a lot of the sql querying from the original codebase.

## About Sails.js and Waterline
http://SailsJs.com

Waterline is a new kind of storage and retrieval engine for Sails.js.  It provides a uniform API for accessing stuff from different kinds of databases, protocols, and 3rd party APIs.  That means you write the same code to get users, whether they live in mySQL, LDAP, MongoDB, or Facebook.
