const { NotFound } = require('@feathersjs/errors');
const { startGetAndLockTransaction } = require('../sessions');

// eslint-disable-next-line no-unused-vars
module.exports = function (options = {}) {
  return async (context) => {
    const { data } = context;

    if (context.method === 'update') {
      const dbData = await startGetAndLockTransaction(
        context,
        options.collections
      );

      if (!dbData) { throw new NotFound(`Can't find ${context.id} in ${context.path}`); }

      const keysToCopy = [
        ...Object.keys(context.service.options.schema.dbSchema),
        'myLock',
        '_id',
        'alias'
      ];
      for (const field of keysToCopy) {
        if (dbData[field]) data[field] = dbData[field];
      }
    }

    return context;
  };
};
