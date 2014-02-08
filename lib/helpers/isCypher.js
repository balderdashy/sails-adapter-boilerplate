
module.exports = function (string) {
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

    ];

    string = string.toLowerCase();

    function containsCypher (element, index, array) {
        o = string.indexOf(element);
        if (o === -1) {
            return false;
        }
        return true;
    }

    return cypherKeywords.some(containsCypher);

};
