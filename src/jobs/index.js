const _ = require('lodash');

const cacheAddressMetrics = require('./cache-address-metrics');
const cacheTokenMetrics = require('./cache-token-metrics');
const convertFees = require('./convert-fees');
const convertProtocolFees = require('./convert-protocol-fees');
const createFills = require('./create-fills');
const deriveFillPrices = require('./derive-fill-prices');
const getMissingTokenImages = require('./get-missing-token-images');
const getNewArticles = require('./get-new-articles');
const measureFills = require('./measure-fills');
const resolveTokens = require('./resolve-tokens');

const jobFns = {
  cacheAddressMetrics,
  cacheTokenMetrics,
  convertFees,
  convertProtocolFees,
  createFills,
  deriveFillPrices,
  getMissingTokenImages,
  getNewArticles,
  measureFills,
  resolveTokens,
};

const getJobs = ({ pollingIntervals }) =>
  _.map(jobFns, (fn, jobName) => ({
    fn,
    maxInterval: pollingIntervals.max[jobName] || pollingIntervals.max.default,
    minInterval: pollingIntervals.min[jobName] || pollingIntervals.min.default,
  }));

module.exports = { getJobs };
