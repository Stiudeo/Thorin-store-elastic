'use strict';
const elasticsearch = require('elasticsearch'),
  url = require('url');
/**
 * Created by Adrian on 09-May-16.
 */
module.exports = function(thorin, opt) {

  const conn = Symbol(),
    options = Symbol(),
    logger = thorin.logger(opt.logger);

  class ESClient {

    constructor(hosts, clientOpt) {
      let esOpt = {
        log: ThorinLogger,
        hosts: []
      };
      this[options] = clientOpt;
      for (let i = 0; i < hosts.length; i++) {
        let item = hosts[i];
        if (typeof item === 'object' && item) {
          esOpt.hosts.push(item);
          continue;
        }
        let tmp = url.parse(item);
        if (!tmp.protocol) {
          tmp.protocol = 'http:';
        }
        if (!tmp.port) tmp.port = (tmp.protocol == 'https:' ? 443 : 80);
        let hostItem = {
          protocol: tmp.protocol,
          host: tmp.hostname,
          port: tmp.port,
          path: tmp.pathname
        }
        esOpt.hosts.push(hostItem);
      }
      esOpt = thorin.util.extend(esOpt, clientOpt);
      this[conn] = new elasticsearch.Client(esOpt);
      //this[conn].log.thorinStarted = false;
      this.hosts = esOpt.hosts;
    }

    /*
     * Pings the server, returning a promise.
     * */
    start() {
      return new Promise((resolve, reject) => {
        this[conn].ping({
          requestTimeout: this[options].requestTimeout
        }, (err) => {
          if(err) {
            return reject(thorin.error('STORE.ES_CLIENT', 'Could not connect to client', err));
          }
          this[conn].transport._config.log.thorinStarted = true;
          this.attach();
          resolve();
        });
      });
    }

    getConnection() {
      return this[conn];
    }

    /*
     * Attaches all the utility functions of the ES Client to the current client wrapper.
     * */
    attach() {
      const clientObj = this[conn],
        INNER_ATTRIBUTES = ['cat', 'cluster', 'indices', 'nodes', 'snapshot'];
    }

  }


  function ThorinLogger(config) {
    function wrapMessage(level) {
      if(!this.thorinStarted) return;
      //console.log(level, arguments);
      //console.log("CONFIG", this.thorinStarted);
    }
    // config is the object passed to the client constructor.
    this.error = wrapMessage.bind(this, 'error');
    this.warning = wrapMessage.bind(this, 'warn');
    this.info = wrapMessage.bind(this, 'info');
    this.debug = wrapMessage.bind(this, 'debug');
    this.trace = wrapMessage.bind(this, 'trace');
    this.close = function() {}
  }

  return ESClient;
}