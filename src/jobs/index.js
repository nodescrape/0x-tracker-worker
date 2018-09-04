const _ = require('lodash');

const createFills = require('./create-fills');
const getMissingTokenImages = require('./get-missing-token-images');
const getMissingTokens = require('./get-missing-tokens');
const getNewArticles = require('./get-new-articles');
const setRelayerForFills = require('./set-relayer-for-fills');
const updateFillPrices = require('./update-fill-prices');
const updateFillRates = require('./update-fill-rates');
const updateFillStatuses = require('./update-fill-statuses');
const updateTokenPrices = require('./update-token-prices');

const jobFns = {
  createFills,
  getMissingTokenImages,
  getMissingTokens,
  getNewArticles,
  setRelayerForFills,
  updateFillPrices,
  updateFillRates,
  updateFillStatuses,
  updateTokenPrices,
};

const getJobs = ({ maxRetries, pollingIntervals }) =>
  _.map(jobFns, (fn, jobName) => ({
    fn,
    interval: pollingIntervals[jobName] || pollingIntervals.default,
    maxRetries: maxRetries[jobName] || maxRetries.default,
  }));

module.exports = { getJobs };
