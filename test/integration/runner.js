/**
 * Test runner dependencies
 */
var mocha = require('mocha');
var TestRunner = require('waterline-adapter-tests');


/**
 * Integration Test Runner
 *
 * Uses the `waterline-adapter-tests` module to
 * run mocha tests against the specified interfaces
 * of the currently-implemented Waterline adapter API.
 */
new TestRunner({

    // Load the adapter module.
    adapter: require('../../'),

    // Default adapter config to use.
    config: {
        schema: false
    },

    // The set of adapter interfaces to test against.
    interfaces: ['semantic', 'queryable']
});
