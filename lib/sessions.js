// Read more:
// https://docs.mongodb.com/manual/core/transactions-in-applications/#transactions-retry
// https://www.mongodb.com/blog/post/how-to-select--for-update-inside-mongodb-transactions
// http://mongodb.github.io/node-mongodb-native/3.6/api/ClientSession.html

const debug = require('debug')('database');
const { nanoid } = require('nanoid');
const { ObjectID } = require('mongodb');
const { TransactionAborted } = require('./exceptions');

// Number of ms to wait before retrying a writeConflict
const writeConflictBase = 50;

const writeConflictJitterMax = 100;

// Number of times to retry to resume a writeConflict.
const writeConflictRetries = 20;

const transactionOptions = {
  readPreference: 'primary',
  readConcern: { level: 'snapshot' },
  writeConcern: { w: 'majority' },
};

let mongoClient = null;
function setClient(client) {
  mongoClient = client;
}

function getClient() {
  return mongoClient;
}

let mongoDatabase = null;
function setDatabase(database) {
  mongoDatabase = database;
}

function getDatabase() {
  return mongoDatabase;
}

const sessions = {};
async function startSession() {
  const sessionId = nanoid();
  sessions[sessionId] = {
    id: sessionId,
    counter: 1,
    session: getClient().startSession(),
  };

  return sessionId;
}

function reuseSession(sessionId) {
  if (!sessions[sessionId]) {
    console.error("Mongo session doesn't exist " + sessionId);
    throw new TransactionAborted(
      "resuseSession: Session doesn't exist",
      sessionId
    );
  }
  sessions[sessionId].counter++;
  return sessionId;
}

function getSession(sessionId) {
  return getSessionObject(sessionId).session;
}

function getSessionCounter(sessionId) {
  return getSessionObject(sessionId).counter;
}

function getSessionObject(sessionId) {
  if (!sessions[sessionId]) {
    // Maybe a service that reused the sessionId failed and deleted the id.
    throw new TransactionAborted(
      "getSessionObject: Session doesn't exist",
      sessionId
    );
  } else {
    return sessions[sessionId];
  }
}

async function endSessionAndCommitTransaction(context) {
  const { params } = context;
  sessions[params.sessionId].counter--;
  if (sessions[params.sessionId].counter === 0) {
    try {
      if (getSession(params.sessionId).inTransaction()) {
        await _commitWithRetry(getSession(params.sessionId));
        debugMsg('commitWithRetry', context);
      }
    } catch (error) {
      console.error('abortTransaction');
      await endSession(params);
      throw error;
    }
    return endSession(params);
  }
}

async function _commitWithRetry(session, retry = 0) {
  try {
    await session.commitTransaction();
  } catch (err) {
    if (err.hasErrorLabel('UnknownTransactionCommitResult')) {
      if (retry >= writeConflictRetries) throw err;
      retry++;
      const timeout = _getExponentialTimeoutWithJitter(retry);
      const counter = getCounter('commitRetry');
      console.error(
        `UnknownTransactionCommitResult, retrying commit operation. retry: ${retry}:${counter}, timeout: ${timeout}`
      );
      await _sleep(timeout);
      await _commitWithRetry(session, retry);
    } else {
      console.error('Error during commit ...');
      throw err;
    }
  }
}

async function endSession(params) {
  const session = getSession(params.sessionId);
  if (session) {
    // Might be in transaction if endSession is called by errorSession hook
    if (session.inTransaction()) {
      await session.abortTransaction();
    }
    await session.endSession();
    delete sessions[params.sessionId];
    delete params.sessionId;
  }
}

async function startGetAndLockTransaction(context, collections, retry = 0) {
  const { params } = context;
  try {
    await _startTransaction(params.sessionId);
    const fromQuery = _getFilterQueriesFromQuery(collections, context.params);
    const fromModel = _getFilterQueriesFromModel(context);
    await _lockDocument('updateOne', params.sessionId, fromQuery);
    const result = await _lockDocument(
      'findOneAndUpdate',
      params.sessionId,
      fromModel
    );

    debugMsg('startGetAndLockTransaction', context);

    if (result.length > 0) return result[0].value;
    else return {};
  } catch (err) {
    if (err.codeName === 'WriteConflict') {
      if (retry >= writeConflictRetries) throw err;
      retry++;
      const timeout = _getExponentialTimeoutWithJitter(retry);
      const counter = getCounter('writeConflict');
      debugMsg(
        `Write conflict (lock) retry: ${retry}:${counter}, timeout: ${timeout}`,
        context
      );
      await _sleep(timeout);
      await getSession(params.sessionId).abortTransaction();
      return await startGetAndLockTransaction(context, collections, retry);
    } else throw err;
  }
}

async function _startTransaction(sessionId) {
  if (getSessionCounter(sessionId) === 1) {
    if (getSession(sessionId).inTransaction()) {
      console.error('Aborting transaction, should it really be started here?');
      await getSession(sessionId).abortTransaction();
    }
    return getSession(sessionId).startTransaction(transactionOptions);
  }
}

async function _lockDocument(operation, sessionId, filterQueries) {
  const lockQuery = { $set: { myLock: ObjectID() } };
  return Promise.all(
    filterQueries.map((v) => {
      return v.collection[operation](v.filterQuery, lockQuery, {
        session: getSession(sessionId),
      });
    })
  );
}

/**
 * Build array with filterQueries from options.Model set in
 * the feathersjs Service. ie. users.class.js
 * Using context.id that is parsed by feathersjs from the uri.
 */
function _getFilterQueriesFromModel(context) {
  if (
    context.service &&
    context.service.options &&
    context.service.options.Model
  ) {
    const options = context.service.options;
    return [
      {
        collection: options.Model,
        filterQuery: { ...context.params.query, [options.id]: context.id },
      },
    ];
  } else {
    return [];
  }
}

/**
 * Get filterQuery based on collection parameter on lock-data hook.
 *
 * @param {*} collections [
 *  {
 *    collection: "users",  // Name of mongodb collection
 *    field: "_id",         // Name of mongodb document field
 *    query: "userId"       // Name of query field in Feathersjs.
 *  }]
 */
function _getFilterQueriesFromQuery(collections, params) {
  if (collections && collections.length > 0) {
    const query = params.query;
    return collections.map((v) => {
      return {
        collection: getDatabase().collection(v.collection),
        filterQuery: { [v.field]: query[v.query] },
      };
    });
  } else {
    return [];
  }
}

function debugMsg(prefix, context, suffix = '') {
  const { sessionId } = context.params;
  let counter = 'NA';
  try {
    counter = getSessionCounter(sessionId);
  } catch (err) {}
  debug(
    `${sessionId} ${prefix}(${counter}) ${context.method} ${getUrl(
      context
    )} ${suffix} `
  );
}

function getUrl(context) {
  let url = context.path;
  for (const key in context.params.query) {
    url = url.replace(':' + key, context.params.query[key]);
  }
  if (context.id) url += `/${context.id}`;
  return url;
}

function _sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function _getExponentialTimeoutWithJitter(retry) {
  const exp = getRandomInt(retry * 0.5) + retry * 0.5;
  const exponent = Math.pow(1.6, exp);
  const rand = getRandomInt(writeConflictJitterMax * retry);
  return Math.floor(writeConflictBase * exponent + rand);
}

function getRandomInt(max) {
  return Math.random() * Math.floor(max);
}

let counters = { writeConflict: 0, commitRetry: 0 };
function getCounter(name) {
  return counters[name]++;
}
module.exports = {
  setClient,
  setDatabase,
  startSession,
  reuseSession,
  endSession,
  getSession,
  getSessionCounter,
  startGetAndLockTransaction,
  endSessionAndCommitTransaction,
  debugMsg,
};
