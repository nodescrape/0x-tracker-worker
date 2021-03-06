const _ = require('lodash');
const mongoose = require('mongoose');
const signale = require('signale');

const { JOB, QUEUE } = require('../constants');
const elasticsearch = require('../util/elasticsearch');
const relayerRegistry = require('../relayers/relayer-registry');

const logger = signale.scope('index fill value');

const isOrderMatcher = relayerId => {
  const relayer = _(relayerRegistry)
    .values()
    .find({ lookupId: relayerId });

  return _.get(relayer, 'orderMatcher', false);
};

const calculateTradeVolume = (value, relayerId) => {
  if (relayerId === null || relayerId === undefined) {
    return 0;
  }

  if (isOrderMatcher(relayerId)) {
    return value / 2;
  }

  return value;
};

const indexFillValue = async job => {
  const { fillId, relayerId, value } = job.data;

  if (!mongoose.Types.ObjectId.isValid(fillId)) {
    throw new Error(`Invalid fillId: ${fillId}`);
  }

  if (!_.isFinite(value)) {
    throw new Error(`Invalid value: ${value}`);
  }

  const exists = await elasticsearch
    .getClient()
    .exists({ id: fillId, index: 'fills', _source: false });
  const indexed = exists.body;

  if (!indexed) {
    throw new Error(`Could not index value of fill: ${fillId}`);
  }

  await elasticsearch.getClient().update({
    id: fillId,
    index: 'fills',
    body: {
      doc: {
        tradeVolume: calculateTradeVolume(value, relayerId),
        updatedAt: new Date(Date.now()).toISOString(),
        value,
      },
    },
  });

  logger.success(`indexed fill value: ${fillId}`);
};

module.exports = {
  fn: indexFillValue,
  jobName: JOB.INDEX_FILL_VALUE,
  queueName: QUEUE.FILL_INDEXING,
};
