'use strict';
/**
 * Created by Adrian on 02-Apr-16.
 */

/*
 * Checks if the given error contains any kind of sequelize information.
 * If it does, we will mutate it so that the error ns is SQL
 * */
function parseError(e) {
  let esError = (e.source || e),
    isMatch = false;
  if (e.code && e.code.indexOf('ES') === 0) {
    isMatch = true;
  } else if (esError.displayName && esError.status && esError.query && esError.path) {
    isMatch = true;
  }
  if(!isMatch) {
    return false;
  }
  e.ns = 'STORE.ES';

  return true;
}

module.exports = parseError;