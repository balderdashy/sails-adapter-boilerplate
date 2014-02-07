
module.exports = function (string) {
    var pathRegex = /[-!$%^&*()+|~=`{}\[\]:";'<>?,.\/]/;
    cypherKeywords = [
        'match',
        'merge',
        'start',
        'where',
        'create',
        'set',
        'delete',
        'remove',
        'foreach'
    ];

    function containsKeywords (element, index, array) {
        o = string.indexOf(element);
        if (o === -1) {
            return false
        }
        return true
    }

    if (pathRegex.test(string) === false && cypherKeywords.some(containsKeywords) === false) {
        return true
    }
    return false
}
