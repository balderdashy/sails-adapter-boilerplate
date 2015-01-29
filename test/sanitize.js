var assert = require('assert');


describe('isCypher should return True or False if the string contains any cypher injection', function () {

    it('Should return True to params object that is cypher injection free', function () {
        var adapter = require('../lib/adapter.js');
        params = {i: 'This is a test' };
        o = adapter.sanitized(null, null, params);
        assert.equal(o, true);

    });

    it('Should return False for params object with Cypher Keywords', function () {
        var adapter = require('../lib/adapter.js');
        params = {i: '/app_model/12_MATCH/12/' };
        o = adapter.sanitized(null, null, params);
        assert.equal(o, false);
    });

    it('Should return False for params object with multiple params and Cypher Keywords', function () {
        var adapter = require('../lib/adapter.js');
        params = {i: '/app_model/COUNT/12/', x: 'something', t: { z: 'test', y: 'START n=node(1)'} };
        o = adapter.sanitized(null, null, params);
        assert.equal(o, false);
    });

    it('Should return True for params object with multiple params and without Cypher Keywords', function () {
        var adapter = require('../lib/adapter.js');
        params = {i: '/app_model/', x: 'something', t: { z: 'test', y: 'something2'} };
        o = adapter.sanitized(null, null, params);
        assert.equal(o, true);
    });
});
