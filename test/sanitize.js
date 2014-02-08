var assert = require('assert'),
isCypher = require('../lib/helpers/isCypher');


describe('isCypher should return True or False if the string contains any cypher injection', function () {

    it(' Should say True to url that is cypher injection free', function () {
        string = 'This is a test';
        o = isCypher(string);
        assert.equal(o, false);

        });

    it('Should catch Cypher Keywords', function () {

        string = '/app_model/12_MATCH/12/';
        o = isCypher(string);
        assert.equal(o, true);
    })
});
