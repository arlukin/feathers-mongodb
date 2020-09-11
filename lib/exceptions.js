class TransactionAborted extends Error {
  constructor (message, sessionId) {
    super(`Transaction aborted (${message}) sessionId ${sessionId}`);
  }
}

module.exports = { TransactionAborted };
