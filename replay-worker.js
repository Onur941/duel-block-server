'use strict';
const { workerData, parentPort } = require('worker_threads');
parentPort.postMessage(require('./engine').replay(workerData.seed, workerData.moves));
