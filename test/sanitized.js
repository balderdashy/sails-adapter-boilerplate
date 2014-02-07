var assert = require('assert'),
isCypherInjectionFree = require('../lib/helpers/isCypherInjectionFree');


describe('isCypherInjectionFree should return True or False if the string contains any cypher injection', function () {

    it(' Should say True to url that is cypher injection free', function () {
        string = 'This is a test';
        o = isCypherInjectionFree(string);
        assert.equal(o, true);

        });

    it('Should catch quotes and non alphanumeric', function () {

        string = "This is aweosme '";
        o = isCypherInjectionFree(string);
        assert.equal(o, false);

    });

    it('Should catch Cypher Keywords', function () {

        string = '/app_model/12_MATCH/12/';
        o = isCypherInjectionFree(string);
        assert.equal(o, false);
    })
});
