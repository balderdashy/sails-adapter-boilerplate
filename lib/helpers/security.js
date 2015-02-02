
module.exports = (function () {
    // A list of cypher related keywords, for sanitization

    cypherKeywords = [
        'match',
        'merge',
        'start',
        'where',
        'create',
        'set',
        'delete',
        'remove',
        'foreach',
        'union',
        'count',
        'return'
    ];

    /**
     * hasCypher() accepts a string, and sees if it contains any of the cypher keywords or special characters
     * @param  {String}  str [A string, that comes from the params object passed into sanitized]
     * @return {Boolean}     [True if it has cypher or special chars, false if it doesn't]
     */
    function hasCypher(str) {
        return cypherKeywords.some(function(element, index, array) {
            var o = str.indexOf(element);
            if (o === -1) {
                return false;
            }
            return true;
        });
    }

    /**
     * sanitized() accepts an object, traverses it, and checks if it contains cypher or special chars
     * @param  {Object} params [An object which could have other objects inside of it]
     * @return {Boolean} truth [True if the object is "Sanitized", false if it isn't]
     */
    function sanitized(connection, collection, params) {
        var i, truth = true;
        for (i in params) {
            if (params.hasOwnProperty(i) && truth) {
                if (typeof params[i] === 'object') {
                    truth = sanitized(connection, collection, params[i]);
                }
                else {
                    if(hasCypher(String(params[i]).toLowerCase())) {
                        truth = false;
                    }
                }
            }
            else {
                break;
            }
        }
        return truth;
    }

    return {
        sanitized: sanitized
    };

})();
