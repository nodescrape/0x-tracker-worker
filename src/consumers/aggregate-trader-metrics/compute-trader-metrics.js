const { getModel } = require('../../model');

const computeTraderMetrics = async (dateFrom, dateTo, granularity) => {
  const results = await getModel('Fill').aggregate([
    {
      $match: {
        date: { $gte: dateFrom, $lte: dateTo },
      },
    },
    {
      $facet: {
        maker: [
          {
            $group: {
              _id: {
                year: { $year: '$date' },
                month: { $month: '$date' },
                day: { $dayOfMonth: '$date' },
                hour: { $hour: '$date' },
                minute:
                  granularity === 'minute' ? { $minute: '$date' } : undefined,
                maker: '$maker',
              },
            },
          },
          {
            $group: {
              _id: {
                year: '$_id.year',
                month: '$_id.month',
                day: '$_id.day',
                hour: '$_id.hour',
                minute: granularity === 'minute' ? '$_id.minute' : undefined,
              },
              makerCount: { $sum: 1 },
            },
          },
          {
            $sort: {
              '_id.year': 1,
              '_id.month': 1,
              '_id.day': 1,
              '_id.hour': 1,
              '_id.minute': 1,
            },
          },
        ],
        taker: [
          {
            $group: {
              _id: {
                year: { $year: '$date' },
                month: { $month: '$date' },
                day: { $dayOfMonth: '$date' },
                hour: { $hour: '$date' },
                minute:
                  granularity === 'minute' ? { $minute: '$date' } : undefined,
                taker: '$taker',
              },
            },
          },
          {
            $group: {
              _id: {
                year: '$_id.year',
                month: '$_id.month',
                day: '$_id.day',
                hour: '$_id.hour',
                minute: granularity === 'minute' ? '$_id.minute' : undefined,
              },
              takerCount: { $sum: 1 },
            },
          },
          {
            $sort: {
              '_id.year': 1,
              '_id.month': 1,
              '_id.day': 1,
              '_id.hour': 1,
              '_id.minute': 1,
            },
          },
        ],
        trader: [
          {
            $addFields: {
              addresses: [
                { type: 'maker', value: '$maker' },
                { type: 'taker', value: '$taker' },
              ],
            },
          },
          {
            $unwind: {
              path: '$addresses',
            },
          },
          {
            $group: {
              _id: {
                year: { $year: '$date' },
                month: { $month: '$date' },
                day: { $dayOfMonth: '$date' },
                hour: { $hour: '$date' },
                minute:
                  granularity === 'minute' ? { $minute: '$date' } : undefined,
                address: '$addresses.value',
              },
            },
          },
          {
            $group: {
              _id: {
                year: '$_id.year',
                month: '$_id.month',
                day: '$_id.day',
                hour: '$_id.hour',
                minute: granularity === 'minute' ? '$_id.minute' : undefined,
              },
              traderCount: { $sum: 1 },
            },
          },
          {
            $sort: {
              '_id.year': 1,
              '_id.month': 1,
              '_id.day': 1,
              '_id.hour': 1,
              '_id.minute': 1,
            },
          },
        ],
      },
    },
  ]);

  const metrics = results[0].trader.map((result, index) => {
    const makerResult = results[0].maker[index];
    const takerResult = results[0].taker[index];
    const date = new Date(
      result._id.year,
      result._id.month,
      result._id.day,
      result._id.hour,
      result._id.minute,
    );

    return {
      date,
      makerCount: makerResult.makerCount,
      takerCount: takerResult.takerCount,
      traderCount: result.traderCount,
    };
  });

  return metrics;
};

module.exports = computeTraderMetrics;
