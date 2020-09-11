const {
  setClient,
  setDatabase,
  startSession,
  reuseSession,
  debugMsg
} = require('../sessions');

/**
 * Start a mongodb session and creates a new sessionId in params
 *
 * @param {*} options {client, database}
 */
module.exports = function (options = {}) {
  setClient(options.client);
  setDatabase(options.database);

  return async (context) => {
    const { params } = context;

    if (params.sessionId) {
      reuseSession(params.sessionId);
      debugMsg('reuseSession', context);
    } else {
      params.sessionId = await startSession();
      debugMsg('startSession', context);
    }
    return context;
  };
};
