// Set Debug mode
process.env.DEBUG = 'exception,database';

const { expect } = require('chai');
const { MongoClient, ObjectID } = require('mongodb');
const feathers = require('@feathersjs/feathers');
const { cloneDeep, omit } = require('lodash');

const service = require('../lib');
const { startSession, endSession, errorSession, lockData } = service.hooks;
const { getSessionCounter } = service.sessions;

const mongoUrl =
  'mongodb://localhost:27017,localhost:27018,localhost:27019/feathers-test';
describe('Feathers MongoDB Service - Transactions', () => {
  const app = feathers();

  let db;
  let mongoClient;

  before(async () => {
    mongoClient = await MongoClient.connect(mongoUrl, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    db = mongoClient.db('feathers-test');
    await Promise.all([
      db.collection('people').removeMany(),
      db.collection('people_details').removeMany(),
    ]);

    // Setup people service
    app.use('/people', service({ events: ['testing'] }));
    const people = app.service('people');
    people.Model = db.collection('people');
    people.hooks({
      before: { all: [startSession({ client: mongoClient, database: db })] },
      after: { all: [endSession()] },
      error: { all: [errorSession()] },
    });

    // Setup people details service
    app.use('/people_details', service({ events: ['testing'] }));
    const peopleDetails = app.service('people_details');
    peopleDetails.Model = db.collection('people_details');
    peopleDetails.hooks({
      before: { all: [startSession({ client: mongoClient, database: db })] },
      after: { all: [endSession()], patch: [detailsAfterHook()] },
      error: { all: [errorSession()] },
    });
  });

  after(async () => {
    return db.dropDatabase().then(() => mongoClient.close());
  });

  describe('Transactions', () => {
    let peopleService, peopleDetailsService, people;
    let startSessionHook, endSessionHook, errorSessionHook, lockDataHook;
    let context;
    let name;

    beforeEach(async () => {
      // Setup hooks
      startSessionHook = startSession({ client: mongoClient, database: db });
      endSessionHook = endSession();
      errorSessionHook = errorSession();
      lockDataHook = lockData({
        collections: [{ collection: 'people', query: 'name', field: 'name' }],
      });

      // Setup test data
      name = 'Ryan';
      context = {
        method: 'post',
        path: 'people',
        params: { query: { name } },
      };

      // Create people test data
      peopleService = app.service('/people');
      people = await Promise.all([
        peopleService.create({ name, age: 0 }),
        peopleService.create({ name: 'AAA' }),
        peopleService.create({ name: 'aaa' }),
        peopleService.create({ name: 'ccc' }),
      ]);

      // Create people_details test data
      peopleDetailsService = app.service('/people_details');
      const _id = people[0]._id;
      peopleDetails = await Promise.all([
        peopleDetailsService.create({ _id, name, age: 0, log: [] }),
        peopleDetailsService.create({ _id: people[1]._id, log: [] }),
        peopleDetailsService.create({ _id: people[2]._id, log: [] }),
        peopleDetailsService.create({ _id: people[3]._id, log: [] }),
      ]);
    });

    afterEach(async () => {
      if (people) {
        await Promise.all([
          peopleService.remove(people[0]._id),
          peopleService.remove(people[1]._id),
          peopleService.remove(people[2]._id),
          peopleService.remove(people[3]._id),
          peopleDetailsService.remove(people[0]._id),
          peopleDetailsService.remove(people[1]._id),
          peopleDetailsService.remove(people[2]._id),
          peopleDetailsService.remove(people[3]._id),
        ]).catch((err) => {});
      }
    });

    it('simple nested start/stop session', async () => {
      const { params } = context;

      await startSessionHook(context);
      expect(getSessionCounter(params.sessionId)).to.equal(1);

      await startSessionHook(context);
      expect(getSessionCounter(params.sessionId)).to.equal(2);

      await startSessionHook(context);
      expect(getSessionCounter(params.sessionId)).to.equal(3);

      const person = await peopleService.create({ name });
      const results = await peopleService.find({
        query: {
          _id: new ObjectID(person._id),
        },
      });

      await endSessionHook(context);
      expect(getSessionCounter(params.sessionId)).to.equal(2);

      await endSessionHook(context);
      expect(getSessionCounter(params.sessionId)).to.equal(1);

      await endSessionHook(context);
      expect(() => getSessionCounter(params.sessionId)).to.throw(
        "Transaction aborted (getSessionObject: Session doesn't exist) sessionId undefined"
      );

      expect(results).to.have.lengthOf(1);

      await peopleService.remove(person._id);
    });

    it('transactions timeout', async function () {
      this.timeout(20000);

      const localContext = cloneDeep(context);
      const { params } = localContext;
      try {
        await startSessionHook(localContext);
        await lockDataHook(localContext);
        console.log(localContext);
        const people = (await peopleService.find(params))[0];
        console.log(people);
        people.timeoutTest = 'timeout';
        await peopleService.patch(people._id, people, params);
        await _sleep(90);

        await endSessionHook(localContext);
        expect(() => getSessionCounter(params.sessionId)).to.throw(
          "Transaction aborted (getSessionObject: Session doesn't exist) sessionId undefined"
        );
      } catch (err) {
        console.log('moasdf');
        localContext.error = { message: err.message };
        await errorSessionHook(localContext);
      }
    });

    function _sleep(ms) {
      return new Promise((resolve) => {
        setTimeout(resolve, ms);
      });
    }

    it('nested start/stop transactions', async () => {
      const query = context.params.query;
      const allPromieses = [];
      let age = 0;
      const numberOfTests = 10;
      do {
        allPromieses.push(doTrans());
      } while (++age < numberOfTests - 1);
      await Promise.all(allPromieses);
      age++;
      await doTrans();
      const people = (await peopleService.find({ query }))[0];
      expect(people).to.have.all.keys('_id', 'name', 'age', 'last', 'myLock');
      expect(people.age).to.equal(age);
      expect(people.last.name).to.equal(name);

      const peopleDetails = await peopleDetailsService.get(people._id);
      expect(peopleDetails.log).to.have.lengthOf(age);
    });

    async function doTrans() {
      const localContext = cloneDeep(context);
      const { params } = localContext;
      const query = params.query;
      try {
        await startSessionHook(localContext);
        await lockDataHook(localContext);

        const people = (await peopleService.find(params))[0];
        const peopleDetails = await peopleDetailsService.get(
          people._id,
          params
        );

        people.age++;
        await peopleService.patch(people._id, people, params);

        // Need to update people before peopleDetails to get the hook to work.
        peopleDetails.log.push(omit(people, ['last', 'myLock']));
        await peopleDetailsService.patch(
          peopleDetails._id,
          peopleDetails,
          params
        );

        await endSessionHook(localContext);
      } catch (err) {
        localContext.error = { message: err.message };
        await errorSessionHook(localContext);
      }
    }
  });
});

const detailsAfterHook = function (options = {}) {
  return async (context) => {
    const { params, app } = context;
    const log = context.data.log;
    const result = await app
      .service('people')
      .patch(context.id, { last: log[log.length - 1] }, params);
    return context;
  };
};
