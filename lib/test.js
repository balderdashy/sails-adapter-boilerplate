var x = require('./adapter');
x.createMany('User',
  {
    params: [
      { name: 'ben' },
      { name: 'joe' }
    ]
  },
  function(err, results) {
    console.log(err, results);
  }
);