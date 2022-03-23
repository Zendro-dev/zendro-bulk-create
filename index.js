const Papa = require("papaparse");

/**
 * Parse a CSV file to queries or mutations and execute them
 * @param {object} file file stream
 * @param {string} model_name model name
 * @param {{string:string}} attributes attributes and corresponding types
 * @param {string} id primary key in the model
 * @param {boolean} isValidation generate validation queries
 * @param {object} globals global environment variables
 * @param {function} execute_graphql function for executing queries or mutations
 */
module.exports.csvProcessing = async (
  file,
  model_name,
  attributes,
  id,
  isValidation,
  globals,
  execute_graphql
) => {
  const { BATCH_SIZE, RECORD_DELIMITER, FIELD_DELIMITER, ARRAY_DELIMITER } =
    globals;
  let recordsBuffer = [];
  let batch_num = 0;
  const isMutation = isValidation ? false : true;

  return new Promise((resolve, reject) => {
    Papa.parse(file, {
      header: true,
      delimiter: FIELD_DELIMITER,
      newline: RECORD_DELIMITER,
      step: async (result, parser) => {
        parser.pause();
        recordsBuffer.push(result.data);
        if (recordsBuffer.length % BATCH_SIZE == 0) {
          const queries = await module.exports.generateQueries(
            recordsBuffer,
            model_name,
            attributes,
            id,
            isMutation,
            ARRAY_DELIMITER
          );
          const result = await module.exports.responseParser(
            queries,
            execute_graphql,
            BATCH_SIZE,
            batch_num
          );
          if (Object.keys(result).length > 0) {
            reject(result);
          }
          recordsBuffer = [];
          batch_num += 1;
        }
        parser.resume();
      },
      complete: async () => {
        const queries = await module.exports.generateQueries(
          recordsBuffer,
          model_name,
          attributes,
          id,
          isMutation,
          ARRAY_DELIMITER
        );
        const result = await module.exports.responseParser(
          queries,
          execute_graphql,
          BATCH_SIZE,
          batch_num
        );
        if (Object.keys(result).length > 0) {
          reject(result);
        }
        resolve();
      },
      error: (err) => {
        reject(err);
      },
    });
  });
};

/**
 * Convert parsed records with JSON format to queries or mutations and execute them
 * @param {object} records parsed records, format as {field: value}
 * @param {string} model_name model name
 * @param {{string:string}} attributes attributes and corresponding types
 * @param {string} id primary key in the model
 * @param {boolean} isValidation generate validation queries
 * @param {object} globals global environment variables
 * @param {function} execute_graphql function for executing queries or mutations
 */
module.exports.jsonProcessing = async (
  records,
  model_name,
  attributes,
  id,
  isValidation,
  globals,
  execute_graphql
) => {
  const { BATCH_SIZE, ARRAY_DELIMITER } = globals;
  const isMutation = isValidation ? false : true;
  let records_num = records.length;
  let batch_num = 0;
  while (records_num > 0) {
    const queries = await module.exports.generateQueries(
      records.slice(batch_num * BATCH_SIZE, (batch_num + 1) * BATCH_SIZE),
      model_name,
      attributes,
      id,
      isMutation,
      ARRAY_DELIMITER
    );
    const result = await module.exports.responseParser(
      queries,
      execute_graphql,
      BATCH_SIZE,
      batch_num
    );
    if (Object.keys(result).length > 0) {
      throw new Error(result);
    }
    batch_num += 1;
    records_num -= BATCH_SIZE;
  }
};

/**
 * Convert parsed records with JSON format to queries or mutations
 * @param {object} records parsed records, format as {field: value}
 * @param {string} model_name model name
 * @param {{string:string}} attributes attributes and corresponding types
 * @param {string} id primary key in the model
 * @param {boolean} isMutation generate mutations
 * @param {string} arrayDelimiter the delimiter for array
 * @returns {string} generated queries or mutations
 */
module.exports.generateQueries = async (
  records,
  model_name,
  attributes,
  id,
  isMutation,
  arrayDelimiter
) => {
  const model_name_uppercase =
    model_name.slice(0, 1).toUpperCase() + model_name.slice(1);
  const non_string_types = ["Int", "Float", "Boolean"];
  let query = isMutation ? "mutation{\n" : "{\n";
  const API = isMutation
    ? `add${model_name_uppercase}`
    : `validate${model_name_uppercase}ForCreation`;
  for (const [index, record] of records.entries()) {
    query += `n${index + 1}: ${API}(`;
    for (const [key, value] of Object.entries(record)) {
      query += `${key}:`;
      let type = attributes[key];
      if (!type) {
        throw new Error(
          `No such field in the model:${key}. Please check the header in the file!`
        );
      } else if (type[0] === "[") {
        const array = value.split(arrayDelimiter);
        if (non_string_types.includes(type.slice(1, type.length - 1))) {
          query += `[${array}],`;
        } else {
          query += `[${array.map((element) => `"${element}"`)}],`;
        }
      } else {
        if (non_string_types.includes(type)) {
          query += `${value},`;
        } else {
          query += `"${value}",`;
        }
      }
    }
    query = query.slice(0, query.length - 1);
    query += isMutation ? `){${id}}\n` : ")\n";
  }
  query += "}";
  return query;
};

/**
 * Execute queries or mutations
 * @param {string} queries queries or mutations
 * @param {function} execute_graphql function for executing queries or mutations
 * @param {number} batch_size batch size
 * @param {number} batch_num the number of batch
 * @returns response with error messages
 */
module.exports.responseParser = async (
  queries,
  execute_graphql,
  batch_size,
  batch_num
) => {
  const response = await execute_graphql(queries);
  let parsed_response = {};
  if (response.errors) {
    let subqueries = queries.split("\n");
    subqueries = subqueries.slice(1, subqueries.length - 1);
    if (response.data) {
      const indices = Object.keys(response.data)
        .filter((key) => response.data[key] === false)
        .map((val) => parseInt(val.slice(1)))
        .sort((a, b) => a - b);
      for (let index of indices) {
        const subquery = subqueries[index - 1];
        const num = batch_size * batch_num + index;
        parsed_response["record" + num] = {
          subquery: subquery,
          errors: response.errors[index - 1],
        };
      }
    } else {
      console.log(response.errors);
      for (let err of response.errors) {
        const index = err.locations[0].line - 1;
        const subquery = subqueries[index - 1];
        const num = batch_size * batch_num + index;
        parsed_response["record" + num] = {
          subquery: subquery,
          errors: err,
        };
      }
    }
  }
  return parsed_response;
};
