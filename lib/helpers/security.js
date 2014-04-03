
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
        'count',
        'return'
    ];
    specialCharReg = /[-!$%^&*()+|~=`{}\[\]:";'<>?,.]/;

    function hasCypher(str) {
        return cypherKeywords.some(function(element, index, array) {
            var o = str.indexOf(element);
            if (o === -1 && !specialCharReg.test(str)) {
                return false;
            }
            return true;
        });
    }

    function sanitized(params) {
        var i, truth = true;
        for (i in params) {
            if (params.hasOwnProperty(i) && truth) {
                if (typeof params[i] === 'object') {
                    truth = sanitized(params[i]);
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
