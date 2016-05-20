'use strict';
const path = require('path'),
  clientInit = require('./client'),
  indexInit = require('./storeIndex');
/**
 * Created by Adrian on 09-May-16.
 */
module.exports = function init(thorin, opt, pluginName) {
  opt = thorin.util.extend({
    logger: pluginName || 'elastic'
  }, opt);
  const config = Symbol(),
    loaded = Symbol(),
    indexCache = Symbol(),
    logger = thorin.logger(opt.logger),
    client = Symbol();
  let ESClient = null,
    StoreIndex = null;


  class ThorinElasticStore extends thorin.Interface.Store {
    static publicName() {
      return "elastic";
    }

    constructor() {
      super();
      this.type = "elastic";
      this[indexCache] = {};  // we cache the getIndex() wrappers for some time.
      this[loaded] = false;
      this[client] = null;
      this[config] = {};
      /* Cleanup timers */
      setInterval(() => {
        this[indexCache] = {};
      }, 60 * 1000);
    }

    /*
    * Expose the actual elasticSearch client to perform manual operations.
    * */
    get client() {
      if(!this[client]) {
        logger.error('ES Client is not ready yet.');
        return null;
      }
      return this[client].getConnection();
    }
    set client(v) {
      logger.error('ES: You cannot explicitly set a client.');
      return;
    }

    /*
     * Initializes the store.
     * */
    init(storeConfig) {
      this[config] = thorin.util.extend({
        debug: {      // Setting this to =false, will disable debugging all together.
          create: true,
          read: true,
          update: true,
          delete: true
        },
        clients: ['http://localhost:9200'],
        options: {
          apiVersion: '2.3',
          sniffOnStart: true,
          sniffInterval: 36000,
          requestTimeout: 10000,
          deadTimeout: 30000,
          maxSockets: 100,
          minSockets: 10
        }
      }, storeConfig);
      if(!ESClient) {
        ESClient = clientInit(thorin, opt);
      }
    }

    /*
     * Check if store is online.
     * */
    run(done) {
      const cObj = new ESClient(this[config].clients, this[config].options);
      cObj.start().then(() => {
        logger.trace('Connected to Elastic Store.');
        this[client] = cObj;
        StoreIndex = indexInit(thorin, opt, this.client);
        done();
      }).catch((e) => {
        logger.error('Failed to connect to Elastic Store', e);
        return done(e);
      });
    }

    /*
    * Creates a new index to ES
    * */
    createIndex(name, opt) {
      let iOpt = {
        index: name.toLowerCase()
      };
      if(typeof opt === 'object' && opt) {
        iOpt.body = opt;
      }
      return new Promise((resolve, reject) => {
        this.client.indices.create(iOpt, (err, r) => {
          if(err && err.message.indexOf('index_already_exists_exception') === -1) {
            return reject(thorin.error('ES.CREATE_INDEX', 'Could not create index', err, 500));
          }
          logger.trace(`Created index ${name}`);
          resolve(this.getIndex(name));
        });
      });
    }

    /*
    * Delete an index.
    * */
    deleteIndex(name) {
      let iOpt = {
        index: (typeof name === 'object' ? name.name : name.toLowerCase())
      }
      return new Promise((resolve, reject) => {
        this.client.indices.delete(iOpt, (e,r) => {
          if(e) {
            return reject(thorin.error('ES.DELETE_INDEX', 'Could not delete index', e, 500));
          }
          resolve(r);
        });
      });
    }

    /*
    * Checks if index exists
    * */
    existsIndex(name, opt) {
      let iOpt = thorin.util.extend({
        index: name.toLowerCase()
      }, opt);
      return new Promise((resolve, reject) => {
        this.client.indices.exists(iOpt, (err, exists) => {
          if(err) {
            return reject(thorin.error('ES.EXIST_INDEX', 'Could not check if index exists', 500, err));
          }
          resolve(exists);
        });
      });
    }

    /*
    * Returns an array of StoreIndexes for all the indices.
    * */
    getIndexes(opt, _rawNames) {
      let iOpt = thorin.util.extend({
        index: []
      }, opt);
      return new Promise((resolve, reject) => {
        this.client.cat.indices(opt, (e, res) => {
          if(e) {
            return reject(thorin.error('ES.GET_INDEXES', 'Could not read store indexes', 500, e));
          }
          if(!res) return resolve([]);
          let items = res.split('\n'),
            result = [],
            len = items.length;
          for(let i=0; i < len; i++) {
            let item = items[i];
            if(item.trim() === '') continue;
            let tmp = item.split(' ');
            if(_rawNames === true) {
              result.push(tmp[2]);
            } else {
              result.push(this.getIndex((tmp[2])));  // this is the index name.
            }

          }
          resolve(result);
        });
      })
    }

    /*
    * Returns an ESIndex wrapper.
    * */
    getIndex(name) {
      if(!name || typeof name !== 'string') return null;
      name = name.toLowerCase();
      if(typeof this[indexCache][name] === 'undefined') {
        this[indexCache][name] = new StoreIndex(name);
      }
      return this[indexCache][name];
    }

  }

  return ThorinElasticStore;

}