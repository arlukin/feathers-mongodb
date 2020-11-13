const { mergeWith, find } = require('lodash');
const { startGetAndLockTransaction } = require('../sessions');

// eslint-disable-next-line no-unused-vars
module.exports = function (options = {}) {
  return async (context) => {
    if (context.method === 'patch') {
      const { data } = context;
      const newData = {};

      const dbData = await startGetAndLockTransaction(
        context,
        options.collections
      );
      mergeWith(newData, dbData, data, customizer);

      context.data = newData;
    }

    return context;
  };
};

function customizer (objValue, srcValue) {
  if (Array.isArray(objValue) && Array.isArray(srcValue)) {
    srcValue.forEach((value) => {
      const objToUpdate = find(objValue, ['_id', value._id]);
      if (objToUpdate) {
        mergeWith(objToUpdate, value, customizer);
      } else {
        objValue.push(value);
      }
    });
    return objValue;
  }
}
