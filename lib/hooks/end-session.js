const { endSessionAndCommitTransaction, debugMsg } = require('../sessions');

// eslint-disable-next-line no-unused-vars
module.exports = function (options = {}) {
  return async (context) => {
    const { params } = context;

    if (!params.sessionId) {
      console.error('No session started', context);
      process.exit();
    }
    debugMsg('endSession', context);
    return endSessionAndCommitTransaction(context);
  };
};
