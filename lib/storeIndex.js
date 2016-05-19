'use strict';
/**
 * Created by Adrian on 10-May-16.
 */
module.exports = function(thorin, opt, clientObj) {

  let logger = thorin.logger(opt.logger);

  class ESIndex {

    constructor(name) {
      this.name = name;
    }

    /*
     * Creates a new document in the current index.
     * Arguments:
     * type - the type of the document
     * data - the associated data.
     * */
    create(type, data, opt) {
      return new Promise((resolve, reject) => {
        if (typeof type !== 'string') {
          logger.error(`Index ${this.name}.save(): type must be string`);
          return reject(thorin.error('ES.CREATE', 'Invalid document type', 400));
        }
        if (typeof data !== 'object' || !data) data = {};
        let payload = thorin.util.extend({
          index: this.name,
          type: type,
          body: data
        }, opt);
        if (typeof data.id !== 'undefined') {
          payload.id = data.id;
        }
        clientObj.index(payload, (e, res) => {
          if (e) {
            logger.warn(`Index ${this.name} could not create type ${type}`);
            logger.debug(e);
            return reject(thorin.error('ES.CREATE', 'Could not store entry.', 500, e));
          }
          resolve(res);
        });
      });
    }

    /*
     * Bulk create items.
     * */
    createBulk(type, items, _opt) {
      return new Promise((resolve, reject) => {
        if (typeof type !== 'string') {
          logger.error(`Index ${this.name}.save(): type must be string`);
          return reject(thorin.error('ES.CREATE_bulk', 'Invalid document type', 400));
        }
        let bodyBulk = thorin.util.extend({
          fields: ['_id'],
          body: []
        }, _opt);
        if (!(items instanceof Array)) items = [items];
        let len = items.length;
        for (let i = 0; i < len; i++) {
          bodyBulk.body.push({
            index: {
              _index: this.name,
              _type: type
            }
          });
          bodyBulk.body.push(items[i]);
        }
        clientObj.bulk(bodyBulk, (e, res) => {
          if (e) {
            logger.warn(`Index ${this.name} could not create type ${type}`);
            logger.debug(e);
            return reject(thorin.error('ES.CREATE_BULK', 'Could not store entries.', 500, e));
          }
          resolve(res);
        });
      });
    }

    /*
     * Tries to find a single document based on its type and id
     * */
    find(type, qry, _rawQry) {
      return new Promise((resolve, reject) => {
        if (typeof type !== 'string') {
          logger.error(`Index ${this.name}.find(): type must be string`);
          return reject(thorin.error('ES.FIND', 'Invalid document type', 400));
        }
        if (typeof qry == 'undefined' || qry == null) qry = {};
        let onlySource = false, includeId = false;
        if (qry.source === true) {
          onlySource = true;
          delete qry.source;
        }
        if(qry.id === true) {
          includeId = true;
          delete qry.id;
        }
        let payload = buildFind(this.name, type, qry, _rawQry);
        payload.size = 1;
        clientObj.search(payload, (err, res) => {
          if (err) {
            logger.warn(`Index ${this.name} could not find type ${type}`);
            return reject(thorin.error('ES.FIND', 'Could not query store for entry', 500, err));
          }
          let hit = res.hits.hits[0],
            hitObj;
          if (onlySource && typeof hit._source !== 'undefined') {
            hitObj = hit._source;
          } else {
            let itemObj = buildHit(hit);
            if(itemObj) {
              hitObj = itemObj;
            }
          }
          if(includeId) {
            hitObj.id = hit._id;
          }
          resolve({
            result: hitObj,
            meta: res.hits
          });
        });
      });
    }

    /*
    * This will count the number of results for the given query.
    * */
    count(type, qry, _rawQry) {
      return new Promise((resolve, reject) => {
        if (typeof type !== 'string') {
          logger.error(`Index ${this.name}.find(): type must be string`);
          return reject(thorin.error('ES.FIND_ALL', 'Invalid document type', 400));
        }
        if (typeof qry == 'undefined' || qry == null) qry = {};
        let payload = buildFind(this.name, type, qry, _rawQry);
        logger.trace(`Count ${this.name}.${type}`, JSON.stringify(payload.body));
        clientObj.count(payload, (err, res) => {
          if (err) {
            // if index does not exist yet, resolve with no item.
            if (err.message.indexOf('index_not_found_exception') !== -1) {
              return resolve(data);
            }
            logger.warn(`Index ${this.name} could not count type ${type}`);
            logger.debug(err);
            return reject(thorin.error('ES.COUNT', 'Could not count store entries', 500, err));
          }
          resolve(res.count);
        });
      });
    }

    /*
     * Tries to find ALL The entries for a given type.
     * IF: qry.source = true, we will resolve ONLY with the _source.
     * IF: qry.id = true, we will add the "id" field to each entry from the _id source
     * */
    findAll(type, qry, _rawQry) {
      return new Promise((resolve, reject) => {
        if (typeof type !== 'string') {
          logger.error(`Index ${this.name}.find(): type must be string`);
          return reject(thorin.error('ES.FIND_ALL', 'Invalid document type', 400));
        }
        if (typeof qry == 'undefined' || qry == null) qry = {};
        let onlySource = false, includeId = false;
        if (qry.source === true) {
          onlySource = true;
          delete qry.source;
        }
        if(qry.id === true) {
          includeId = true;
          delete qry.id;
        }
        let payload = buildFind(this.name, type, qry, _rawQry);
        logger.trace(`FindAll ${this.name}.${type}`, JSON.stringify(payload.body));
        clientObj.search(payload, (err, res) => {
          let data = {
            result: [],
            meta: {}
          };
          if (err) {
            // if index does not exist yet, resolve with no item.
            if (err.message.indexOf('index_not_found_exception') !== -1) {
              return resolve(data);
            }
            logger.warn(`Index ${this.name} could not findAll type ${type}`);
            return reject(thorin.error('ES.FIND_ALL', 'Could not query store for entries', 500, err));
          }
          try {
            let len = res.hits.hits.length;
            for (let i = 0; i < len; i++) {
              let hit = res.hits.hits[i],
                hitObj;
              if (onlySource && typeof hit._source !== 'undefined') {
                hitObj = hit._source;
              } else {
                let itemObj = buildHit(hit);
                if (!itemObj) continue;
                hitObj = itemObj;
              }
              if(includeId) {
                hitObj.id = hit._id;
              }
              data.result.push(hitObj);
            }
          } catch (e) {
          }
          // set pagination data.
          data.meta = {
            total_count: res.hits.total
          };
          if (payload.body.size) {
            data.meta.page_count = Math.ceil(data.meta.total_count / payload.body.size);
            if (payload.body.from) {
              let page = Math.floor(payload.body.from / payload.body.size);
              data.meta.current_page = page + 1;
            }
          }
          data.meta.current_count = data.result.length;
          resolve(data);
        });
      });
    }

    /*
     * Updaets the given document
     * */
    update(type, id, data, _opt) {
      return new Promise((resolve, reject) => {
        if (typeof type === 'object' && type && type._type) {
          id = type._id || type.id;
          type = type._type;
          data = id;
          _opt = data;
        } else if (typeof id === 'object' && id) {
          id = (id._id || id.id);
        }
        if (typeof type !== 'string') {
          logger.error(`Index ${this.name}.destroy(): type must be string`);
          return reject(thorin.error('ES.DESTROY', 'Invalid document type', 400));
        }
        if (typeof id !== 'string' && typeof id !== 'number') {
          logger.error(`Index ${this.name}.destroy(): id must be string or number`);
          return reject(thorin.error('ES.DESTROY', 'Invalid document id', 400));
        }
        let payload = {
          index: this.name,
          type,
          id,
          body: {}
        }
        if (typeof data.script !== 'undefined' || typeof data.upsert !== 'undefined') {
          payload.body = data;
        } else {
          payload.body = {
            doc: data
          };
        }
        if (typeof _opt === 'object' && _opt) {
          payload = thorin.util.extend(payload, _opt);
        }
        clientObj.update(payload, (err, res) => {
          if (err) {
            logger.warn(`Index ${this.name} cold not update type ${type} id ${id}`);
            return reject(thorin.error('ES.UPDATE', 'Could not persist updates', 500, err));
          }
          resolve(res);
        });
      });
    }

    /*
     * Destroys a document or based on its id.
     * */
    destroy(type, id) {
      return new Promise((resolve, reject) => {
        if (typeof type === 'object' && type && type._type) {
          id = type._id || type.id;
          type = type._type;
        }
        if (typeof type !== 'string') {
          logger.error(`Index ${this.name}.destroy(): type must be string`);
          return reject(thorin.error('ES.DESTROY', 'Invalid document type', 400));
        }
        if (typeof id === 'object' && id) {
          id = (id._id || id.id);
        }
        if (typeof id !== 'string' && typeof id !== 'number') {
          logger.error(`Index ${this.name}.destroy(): id must be string or number`);
          return reject(thorin.error('ES.DESTROY', 'Invalid document id', 400));
        }
        let payload = {
          index: this.name,
          type,
          id
        };
        return clientObj.delete(payload, (err, res) => {
          if (err && err.status !== 404) {
            logger.warn(`Index ${self.name} could not destroy type ${type}`);
            return reject(thorin.error('ES.DESTROY', 'Could not delete entries', 500, err));
          }
          resolve(res);
        });
      });
    }
  }


  /*
   * Builds a find payload based on the query.
   * */
  function buildFind(index, type, qry, _rawQry) {
    if (_rawQry === true) {
      qry.index = index;
      qry.type = type;
      return qry;
    }
    let payload = {
      index,
      type,
      body: {
        query: {}
      }
    }
    if (typeof qry !== 'object' || !qry) {
      if (typeof qry === 'string' || typeof qry === 'number') {
        payload.body.query.ids = {
          values: [qry]
        }
      }
      return payload;
    }
    if (typeof qry.where === 'object' && Object.keys(qry.where).length === 0) {
      delete qry.where;
    }
    let qryKeys = Object.keys(qry);
    for (let i = 0; i < qryKeys.length; i++) {
      let keyName = qryKeys[i],
        keyValue = qry[keyName];
      switch (keyName) {
        case 'where':
          if ((keyValue.match && keyValue.range) ||
            (keyValue.terms && keyValue.range) ||
            (keyValue.match && keyValue.terms)) {
            payload.body.query = {
              bool: {
                must: []
              }
            }
            Object.keys(keyValue).forEach((k) => {
              let itm = {};
              itm[k] = keyValue[k];
              payload.body.query.bool.must.push(itm);
            });
          } else {
            if (typeof keyValue['range'] !== 'undefined') {
              payload.body.query.range = keyValue.range;
            } else if (typeof keyValue['terms'] !== 'undefined') {
              payload.body.query.terms = keyValue['term'];
            } else if (typeof keyValue['match'] !== 'undefined') {
              payload.body.query.match = keyValue['match'];
            } else {
              payload.body.query.match = keyValue;
            }
          }
          break;
        case 'limit':
          payload.body.size = keyValue;
          break;
        case 'offset':
          payload.body.from = keyValue;
          break;
        case 'order':
          payload.body.sort = [];
          buildSort(payload.body.sort, keyValue);
          break;
        case 'attributes':
          payload.body.fields = keyValue;
          break;
        case 'raw': // RAW queries will be merged with payload.body
          payload.body = Object.assign(payload.body, keyValue);
          break;
        default:
          payload.body[keyName] = keyValue;
      }
    }
    if (Object.keys(payload.body.query).length === 0) {
      delete payload.body.query;
    }
    return payload;
  }

  /*
   * Builds the sort array field, from a sequelize-like where.
   * */
  function buildSort(result, sort) {
    if (typeof sort === 'string') {
      result.push(sort);
      return;
    }
    if (sort instanceof Array) {
      if (typeof sort[0] === 'string') {
        for (let i = 0; i < sort.length; i = i + 2) {
          let field = sort[i],
            dir = sort[i + 1].toLowerCase();
          let item = {};
          item[field] = {
            order: dir,
            unmapped_type: true
          };
          result.push(item);
        }
      } else {
        for (let i = 0; i < sort.length; i++) {
          if (sort[i] instanceof Array) {
            buildSort(result, sort[i]);
            continue;
          }
          if (typeof sort[i] === 'object') {
            result.push(sort[i]);
            continue;
          }
        }
      }
    }
  }

  /*
   * Builds a hit item source
   * */
  function buildHit(item) {
    let resObj;
    try {
      delete item.sort;
      if (!item.fields) {  // we have no fields in the search.
        resObj = item;
      } else {
        // we do have fields, we must extract them.
        resObj = item;
        Object.keys(item.fields).forEach((fieldName) => {
          resObj[fieldName] = item.fields[fieldName];
        });
        delete resObj.fields;
      }
    } catch (e) {
      resObj = null;
    }
    return resObj;
  }

  return ESIndex;
}
;