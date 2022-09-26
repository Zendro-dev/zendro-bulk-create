const Papa = require("papaparse");
const inflection = require("inflection");
const equal = require("fast-deep-equal");
/**
 * Parse a CSV file to queries or mutations and execute them
 * @param {object} file file stream
 * @param {json} dataModelDefinition data model Definition
 * @param {boolean} isValidation generate validation queries
 * @param {object} globals global environment variables
 * @param {function} execute_graphql function for executing queries or mutations
 */
module.exports.csvProcessing = async (
  file,
  dataModelDefinition,
  isValidation,
  globals,
  execute_graphql
) => {
  const { RECORD_DELIMITER, FIELD_DELIMITER, ARRAY_DELIMITER } = globals;
  const BATCH_SIZE = globals.LIMIT_RECORDS ?? globals.MAX_RECORD_LIMIT;
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
            dataModelDefinition,
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
            dataModelDefinition,
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
 * @param {json} dataModelDefinition data model Definition
 * @param {boolean} isValidation generate validation queries
 * @param {object} globals global environment variables
 * @param {function} execute_graphql function for executing queries or mutations
 */
module.exports.jsonProcessing = async (
  records,
  dataModelDefinition,
  isValidation,
  globals,
  execute_graphql
) => {
  const { ARRAY_DELIMITER } = globals;
  const BATCH_SIZE = globals.LIMIT_RECORDS ?? globals.MAX_RECORD_LIMIT;
  const isMutation = isValidation ? false : true;
  let records_num = records.length;
  let batch_num = 0;
  while (records_num > 0) {
    const queries = await module.exports.generateQueries(
      records.slice(batch_num * BATCH_SIZE, (batch_num + 1) * BATCH_SIZE),
      dataModelDefinition,
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
 * @param {json} dataModelDefinition data model Definition
 * @param {boolean} isMutation generate mutations
 * @param {string} arrayDelimiter the delimiter for array
 * @returns {string} generated queries or mutations
 */
module.exports.generateQueries = async (
  records,
  dataModelDefinition,
  isMutation,
  arrayDelimiter
) => {
  const modelName = dataModelDefinition.model;
  const model_name_uppercase =
    modelName.slice(0, 1).toUpperCase() + modelName.slice(1);
  const id = dataModelDefinition.id.name;
  const non_string_types = ["Int", "Float", "Boolean"];
  let query = isMutation ? "mutation{\n" : "{\n";
  const API = isMutation
    ? `add${model_name_uppercase}`
    : `validate${model_name_uppercase}ForCreation`;
  const attributes = dataModelDefinition.attributes;
  let addAssociations = {};
  const associations = dataModelDefinition.associations;
  for (const [assocName, assocObj] of Object.entries(associations)) {
    let addAssocName =
      "add" + assocName.slice(0, 1).toUpperCase() + assocName.slice(1);
    if (assocObj.sourceKey) {
      addAssociations[addAssocName] = attributes[assocObj.sourceKey];
    } else if (assocObj.keysIn === modelName) {
      addAssociations[addAssocName] = attributes[assocObj.targetKey];
    }
  }
  for (const [index, record] of records.entries()) {
    query += `n${index + 1}: ${API}(`;
    for (const [key, value] of Object.entries(record)) {
      let type = attributes[key] ?? addAssociations[key];
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
            query += `${key}:[${array}],`;
          } else {
            query += `${key}:[${array.map(
              (element) => `${JSON.stringify(element)}`
            )}],`;
          }
        } else {
          const quoted = value[0] == '"' && value[value.length - 1] == '"';
          if (non_string_types.includes(type)) {
            query += `${key}:${quoted ? JSON.parse(value) : value},`;
          } else {
            query += `${key}:${quoted ? value : JSON.stringify(value)},`;
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
  let parsed_response = {};
  let response;
  try {
    response = await execute_graphql(queries);
  } catch (err) {
    response = err.response;
  }
  if (response.errors) {
    let subqueries = queries.split("\n");
    subqueries = subqueries.slice(1, subqueries.length - 1);
    if (response.data) {
      let map_queries = {};
      const inputs = response.errors.map(
        (err_obj) => err_obj.input ?? err_obj.extensions.input
      );
      for (let [query_index, query] of Object.entries(subqueries)) {
        let split_res = query.split(": ");
        if (response.data[split_res[0]] === false) {
          let values = split_res[1].split("(")[1].split(")")[0].split(",");
          let new_val = {};
          for (let value of values) {
            let [key, val] = value.split(":");
            new_val[key] = JSON.parse(val);
          }
          for (let [err_index, input] of Object.entries(inputs)) {
            if (equal(input, new_val)) {
              map_queries[query_index] = err_index;
            }
          }
        }
      }
      const indices = Object.keys(map_queries)
        .map((val) => parseInt(val))
        .sort((a, b) => a - b);
      for (const query_index of indices) {
        const num = batch_size * batch_num + query_index + 1;
        parsed_response["record" + num] = {
          errors: response.errors[parseInt(map_queries[query_index])],
        };
      }
    } else {
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
 * @param {[string]} attributes attributes corresponding to the CSV header
 * @param {object} globals global environment variables
 * @param {function} execute_graphql function for executing queries or mutations
 * @param {boolean} is_browser is in the browser environment
 * @param {string} file_path path for saving the CSV file
 */
module.exports.bulkDownload = async (
  model_name,
  header,
  attributes,
  globals,
  execute_graphql,
  is_browser,
  file_path
) => {
  try {
    const { RECORD_DELIMITER, FIELD_DELIMITER, ARRAY_DELIMITER } = globals;
    const BATCH_SIZE = globals.LIMIT_RECORDS ?? globals.MAX_RECORD_LIMIT;

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
      const fs = require("fs");
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
    while (hasNextPage) {
      let data = await execute_graphql(
        `{${connection_resolver}( pagination: {first:${batch_step.first}${
          batch_step.after ? ', after:"' + batch_step.after + '"' : ""
        }}){
        pageInfo{
          hasNextPage
          endCursor
        }
        ${inflection.pluralize(
          model_name.slice(0, 1).toLowerCase() + model_name.slice(1)
        )} {
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
        row = row.slice(0, -1);
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
