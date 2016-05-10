'use strict';
const initStore = require('./lib/elasticStore');
/**
 * Created by Adrian on 29-Mar-16.
 * Events:
 */
module.exports = function init(thorin, opt) {
  const async = thorin.util.async;
  // Attach the SQL error parser to thorin.
  thorin.addErrorParser(require('./lib/errorParser'));
  const ThorinElasticStore = initStore(thorin, opt);

  return ThorinElasticStore;
};
module.exports.publicName = 'elastic';