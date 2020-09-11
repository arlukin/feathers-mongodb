const { startGetAndLockTransaction } = require('../sessions');

// eslint-disable-next-line no-unused-vars
module.exports = function (options = {}) {
  return async (context) => {
    await startGetAndLockTransaction(context, options.collections);

    return context;
  };
};
