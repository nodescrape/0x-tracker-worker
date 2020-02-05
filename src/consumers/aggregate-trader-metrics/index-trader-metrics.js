const elasticsearch = require('../../util/elasticsearch');

const indexTraderMetrics = async (metrics, granularity) => {
  const body = metrics
    .map(metric => {
      return [
        JSON.stringify({
          index: {
            _id: `${metric.date.getTime()}-${granularity}`,
          },
        }),
        JSON.stringify({
          doc: {
            ...metric,
            granularity,
          },
        }),
      ].join('\n');
    })
    .join('\n');

  await elasticsearch
    .getClient()
    .bulk({ body: `${body}\n`, index: 'network-metrics' });
};

module.exports = indexTraderMetrics;
