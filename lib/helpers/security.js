
module.exports = (function () {
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
    specialCharReg = /[-!$%^&*()+|~=`{}\[\]:";'<>?,.]/;

    function hasCypher (param) {
        return cypherKeywords.some(function(element, index, array) {
            var o = param.indexOf(element);
            if (o === -1 && !specialCharReg.test(element)) {
                return false;
            }
            return true;
        });
    }

    function sanitized(params) {
        var truth = true;
        params.forEach(function(element, index, array) {
            if (truth) {
                if(hasCypher(element.toLowerCase())) {
                    truth = false;
                }
            }
        });

        return truth;
    }

    return {
        sanitized: sanitized
    };

})();
