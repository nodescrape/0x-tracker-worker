const bluebird = require('bluebird');
const ms = require('ms');
const signale = require('signale');

const {
  MissingBlockError,
  UnsupportedAssetError,
  UnsupportedProtocolError,
} = require('../../errors');
const { JOB, QUEUE } = require('../../constants');
const { publishJob } = require('../../queues');
const createFill = require('./create-fill');
const ensureTokenExists = require('../../tokens/ensure-token-exists');
const Event = require('../../model/event');
const persistFill = require('./persist-fill');
const withTransaction = require('../../util/with-transaction');

const logger = signale.scope('create fills');

const SUPPORTED_VERSIONS = [1, 2, 3];

const createFills = async ({ batchSize }) => {
  const events = await Event.find({
    fillCreated: { $in: [false, null] },
    protocolVersion: { $in: SUPPORTED_VERSIONS },
  }).limit(batchSize);

  logger.info(`found ${events.length} events without associated fills`);

  await bluebird.mapSeries(events, async event => {
    logger.time(`create fill for event ${event.id}`);

    try {
      const fill = await createFill(event);

      // Ensure any new tokens are added to the tokens collection
      await Promise.all(
        fill.assets.map(async asset => {
          if (await ensureTokenExists(asset.tokenAddress, asset.tokenType)) {
            logger.success(`created token: ${asset.tokenAddress}`);
          }
        }),
      );

      await withTransaction(async session => {
        logger.time(`persist fill for event ${event._id}`);
        const newFill = await persistFill(session, event, fill);
        logger.timeEnd(`persist fill for event ${event._id}`);

        await publishJob(
          QUEUE.FILL_PROCESSING,
          JOB.FETCH_FILL_STATUS,
          {
            fillId: newFill._id,
            transactionHash: newFill.transactionHash,
          },
          {
            delay: ms('5 seconds'), // Delay status fetching to ensure MongoDB changes have propagated
          },
        );

        await publishJob(
          QUEUE.FILL_INDEXING,
          JOB.INDEX_FILL,
          {
            fillId: newFill._id,
          },
          {
            delay: ms('5 seconds'), // Delay indexing to ensure MongoDB changes have propagated
          },
        );
      });

      logger.timeEnd(`create fill for event ${event.id}`);
    } catch (error) {
      if (error instanceof MissingBlockError) {
        logger.warn(
          `Unable to create fill for event ${event.id} due to missing block`,
        );
      } else if (error instanceof UnsupportedAssetError) {
        logger.warn(
          `Unable to create fill for event ${event.id} due to unsupported asset`,
        );
      } else if (error instanceof UnsupportedProtocolError) {
        logger.warn(
          `Unable to create fill for event ${event.id} due to unsupported protocol`,
        );
      } else {
        throw error;
      }
    }
  });
};

module.exports = createFills;
