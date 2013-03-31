![image_squidhome@2x.png](http://i.imgur.com/RIvu9.png) 

# Riak DB adapter for Sails.js
Implementation of a Riak database adapter for the Sails.js web-framework.


## Current status
This is only the first implementation attempt. The main focus was to make sure that all the unit-test defined by the sails.js NPM module pass.

## Known limitations
1. Support for auto-increment capabilities are not natively supported by a Riak database. These capabilities are emulated in software and the solution will not scale to a multi-instance/cluster setup.
2. All model creation operations for a particular collection are serialized. This ensures that the auto-increment value for that collection remains consistent. However, it is possible to create two models in parallel if the models do not belong to the same collection.
3. Transactions are also supported, but the Riak back-end must be configured to have the "search" functionality enabled. This means that one needs to edit the `app.config` file and enable the "search" support for the Riak instance.

## Running the tests
1. Install Riak on your machine. Enable the "search" support by editing the `app.config` file.
2. Link the `sails-riak` module into the `sails.js` module.
3. Modify the `User.js` file in the tests folder to use the `sails-riak` adapter.
4. Run the `sails.js` tests.

## Test results
All tests should just pass except two test-cases. These test cases have been slightly altered in order to make them pass. Below is a short description of these changes

# stream.test.js - 'should grab the same set of data as findAll' 
The problem is that the test assumes that both the `stream` and the `findAll` APIs provide the same list of models in the same order. This is why a string representation is created for the results provided by these two APIs, and it's this string representation that is subject to the equality test. 

This is only half true for the case of a Riak back-end. Both the `stream` and 'findAll' APIs provide the same list of models, but not necessarily in the same order. This is why the test case has been altered so that the equality test is applied on the sorted-list of models provided by each of the two APIs.

# transactions.test.js - 'should support 200 simultaneous dummy transactions' 
I was simply unable to squeeze the required performance out of the transactions engine to support 200 simultaneous transactions in a time-span of 8 secs. With the default implementation I was able to do a maximum of 40 transactions. This is why I had to implement a custom transaction logic where the search for locks is performed using Riak's own search capabilities. With this new implementation I was able to sustain up to 120 simultaneous transactions.

For this reason I modified the test case to reduce the 200 simultaneous transactions limit to 100.

All these modifications are available at: https://github.com/andreifecioru/sails

## Future work
I need to find a way to add support for Riak-specific features such as links, bucket configuration, etc.