const Papa = require("papaparse");
const inflection = require("inflection");

/**
 * Parse a CSV file to queries or mutations and execute them
 * @param {object} file file stream
 * @param {json} dataModelDefiniton data model definiton
 * @param {boolean} isValidation generate validation queries
 * @param {object} globals global environment variables
 * @param {function} execute_graphql function for executing queries or mutations
 */
module.exports.csvProcessing = async (
  file,
  dataModelDefiniton,
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
            dataModelDefiniton,
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
        if (recordsBuffer.length > 0) {
          const queries = await module.exports.generateQueries(
            recordsBuffer,
            dataModelDefiniton,
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
 * @param {json} dataModelDefiniton data model definiton
 * @param {boolean} isValidation generate validation queries
 * @param {object} globals global environment variables
 * @param {function} execute_graphql function for executing queries or mutations
 */
module.exports.jsonProcessing = async (
  records,
  dataModelDefiniton,
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
      dataModelDefiniton,
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
 * @param {json} dataModelDefiniton data model definiton
 * @param {boolean} isMutation generate mutations
 * @param {string} arrayDelimiter the delimiter for array
 * @returns {string} generated queries or mutations
 */
module.exports.generateQueries = async (
  records,
  dataModelDefiniton,
  isMutation,
  arrayDelimiter
) => {
  const modelName = dataModelDefiniton.model;
  const model_name_uppercase =
    modelName.slice(0, 1).toUpperCase() + modelName.slice(1);
  const id = dataModelDefiniton.id.name;
  const non_string_types = ["Int", "Float", "Boolean"];
  let query = isMutation ? "mutation{\n" : "{\n";
  const API = isMutation
    ? `add${model_name_uppercase}`
    : `validate${model_name_uppercase}ForCreation`;
  const attributes = dataModelDefiniton.attributes;
  let foreignKeyObj = {};
  const associations = dataModelDefiniton.associations;
  for (const [assocName, assocObj] of Object.entries(associations)) {
    if (assocObj.keysIn === modelName) {
      foreignKeyObj[assocObj.targetKey] =
        "add" + assocName.slice(0, 1).toUpperCase() + assocName.slice(1);
    }
  }
  let foreignKeys = Object.keys(foreignKeyObj);
  for (const [index, record] of records.entries()) {
    query += `n${index + 1}: ${API}(`;
    for (const [key, value] of Object.entries(record)) {
      const field =
        foreignKeys && foreignKeys.includes(key) ? foreignKeyObj[key] : key;
      let type = attributes[key];
      try {
        if (!type) {
          throw new Error(
            `No such field in the model:${key}. Please check the header in the file!`
          );
        } else if (value == "NULL" || value == '"NULL"' || value == "") {
          continue;
        } else if (type[0] === "[") {
          const quoted = value[0] == '"' && value[value.length - 1] == '"';
          const array = quoted
            ? JSON.parse(value).split(arrayDelimiter)
            : value.split(arrayDelimiter);
          if (non_string_types.includes(type.slice(1, type.length - 1))) {
            query += `${field}:[${array}],`;
          } else {
            query += `${field}:[${array.map(
              (element) => `${JSON.stringify(element)}`
            )}],`;
          }
        } else {
          const quoted = value[0] == '"' && value[value.length - 1] == '"';
          if (non_string_types.includes(type)) {
            query += `${field}:${quoted ? JSON.parse(value) : value},`;
          } else {
            query += `${field}:${quoted ? value : JSON.stringify(value)},`;
          }
        }
      } catch (error) {
        throw new Error(error);
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

/**
 * Download all records for a model by batches
 * @param {string} model_name model name
 * @param {string} header CSV header
 * @param {object} globals global environment variables
 * @param {function} execute_graphql function for executing queries or mutations
 * @param {boolean} is_browser is in the browser environment
 * @param {string} file_path path for saving the CSV file
 */
module.exports.bulkDownload = async (
  model_name,
  header,
  globals,
  execute_graphql,
  is_browser,
  file_path
) => {
  try {
    const { BATCH_SIZE, RECORD_DELIMITER, FIELD_DELIMITER, ARRAY_DELIMITER } =
      globals;

    //get connection resolver
    let connection_resolver =
      inflection.pluralize(
        model_name.slice(0, 1).toLowerCase() + model_name.slice(1)
      ) + "Connection";

    //get count resolver
    let count_resolver =
      "count" +
      inflection.pluralize(
        model_name.slice(0, 1).toUpperCase() + model_name.slice(1)
      );
    let total_records = await execute_graphql(`{${count_resolver}}`);

    total_records = is_browser
      ? total_records[count_resolver]
      : total_records.data[count_resolver] ??
        total_records.data.data[count_resolver];

    console.log(`Start to download ${total_records} records`);
    let writableStream = [];
    if (!is_browser) {
      writableStream = fs.createWriteStream(file_path);
    }

    if (is_browser) {
      writableStream.push(header + RECORD_DELIMITER);
    } else {
      writableStream.write(header + RECORD_DELIMITER);
    }

    //pagination
    let batch_step = {
      first: BATCH_SIZE,
    };

    let hasNextPage = total_records > 0;

    //get attributes names
    let attributes = header.split(",");

    while (hasNextPage) {
      let data = await execute_graphql(
        `{${connection_resolver}( pagination: {first:${batch_step.first}${
          batch_step.after ? ', after:"' + batch_step.after + '"' : ""
        }}){
        pageInfo{
          hasNextPage
          endCursor
        }
        countries {
          ${attributes}
        }        
      }}`
      );
      data = is_browser
        ? data[connection_resolver]
        : data.data[connection_resolver] ?? data.data.data[connection_resolver];

      let nodes = data[inflection.pluralize(model_name)];
      hasNextPage = data.pageInfo.hasNextPage;
      batch_step["after"] = data.pageInfo.endCursor;

      for await (record of nodes) {
        let row = "";
        attributes.forEach((attr) => {
          if (
            record[attr] === null ||
            record[attr] === undefined ||
            (Array.isArray(record[attr]) && record[attr].length === 0)
          ) {
            row += `"NULL"${FIELD_DELIMITER}`;
          } else {
            row += Array.isArray(record[attr])
              ? '"' + record[attr].join(`${ARRAY_DELIMITER}`) + '"'
              : '"' + record[attr] + '"';
            row += `${FIELD_DELIMITER}`;
          }
        });
        if (is_browser) {
          writableStream.push(row + RECORD_DELIMITER);
        } else {
          writableStream.write(row + RECORD_DELIMITER);
        }
      }
    }
    if (is_browser) {
      return writableStream;
    }
  } catch (error) {
    throw new Error(error);
  }
};
