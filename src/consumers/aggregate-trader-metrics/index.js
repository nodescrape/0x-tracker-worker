const signale = require('signale');

const { JOB, QUEUE } = require('../../constants');
const computeTraderMetrics = require('./compute-trader-metrics');
const indexTraderMetrics = require('./index-trader-metrics');
const withTimer = require('../../util/with-timer');

const logger = signale.scope('aggregate trader metrics');

const aggregateTraderMetrics = async job => {
  const { dateFrom, dateTo, granularity } = job.data;

  if (dateFrom === undefined) {
    throw new Error(`Invalid dateFrom: ${dateFrom}`);
  }

  if (dateTo === undefined) {
    throw new Error(`Invalid dateFrom: ${dateTo}`);
  }

  if (!['minute', 'hour'].includes(granularity)) {
    throw new Error(`Granularity is invalid: ${granularity}`);
  }

  const parsedDateFrom = new Date(dateFrom);
  const parsedDateTo = new Date(dateTo);

  await withTimer(
    logger,
    `aggregate trader metrics for ${parsedDateFrom.toISOString()} to ${parsedDateTo.toISOString()} with ${granularity} granularity`,
    async () => {
      const metrics = await computeTraderMetrics(
        parsedDateFrom,
        parsedDateTo,
        granularity,
      );

      await indexTraderMetrics(metrics, granularity);
    },
  );
};

module.exports = {
  fn: aggregateTraderMetrics,
  jobName: JOB.AGGREGATE_TRADER_METRICS,
  queueName: QUEUE.METRICS_AGGREGATION,
};
