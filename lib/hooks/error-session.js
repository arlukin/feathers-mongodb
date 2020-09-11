const { endSession, debugMsg } = require('../sessions');

// eslint-disable-next-line no-unused-vars
module.exports = function (options = {}) {
  return async (context) => {
    debugMsg('errorSession ', context, context.error.message);

    if (context.error.data && context.error.data.code !== 11000) {
      console.error(
        'errorSession',
        context.path,
        JSON.stringify(context.data, null, '  '),
        JSON.stringify(context.error, null, '  ')
      );
    }

    await endSession(context.params);

    return context;
  };
};
