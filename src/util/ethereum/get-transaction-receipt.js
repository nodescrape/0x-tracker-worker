const web3 = require('./web3');

const getTransactionReceipt = async transactionHash => {
  const receipt = await web3
    .getWrapper()
    .getTransactionReceiptAsync(transactionHash);

  return receipt;
};

module.exports = getTransactionReceipt;
