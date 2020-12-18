const parseUrl = require("url-parse");
const https = require("https");
const nanoid = require("nanoid");
const program = require("commander");
const _ = require("lodash");

const AthenaDAO = require("./AthenaDAO");
const config = require("./config.json");

const metadataId = config.metadataId;
const qrveyPostdataUrl = config.postdataurl;
const qrveyApiKey = config.apikey;

const generateNanoId = nanoid.customAlphabet("1234567890abcdefghijklmnopqrstuvxyz", 10);

program.version("0.0.1").option("-q, --query <query>", "SQL Query").parse(process.argv);

if (!program.query && _.isEmpty(config.query)) {
  throw new Error('SQL Query is required. Use the command line i.e. node index.js -q "Select * from table" or update the parameter query inside file called config.json.');
}

const sql = program.query ? program.query : config.query;

const _createCTAQuery = (ctasTableName, sqlQuery) => {
  return `
    CREATE TABLE ${ctasTableName}
        WITH (
            format = 'JSON',
            bucket_count=100,
            bucketed_by=ARRAY['dr_random_athena_bucket']
            )
        AS

    ${sqlQuery}
    `;
};

const sqlToExecute = `
    Select *,RANDOM() as dr_random_athena_bucket from
   (
       ${sql}
   ) 
`;

const run = async () => {
  const ctaTable = `cts_${generateNanoId()}`;
  //Build query
  const sqlQuery = _createCTAQuery(ctaTable, sqlToExecute);
  //Execute query and get the queryExecutionId
  const queryExecutionId = await AthenaDAO.executeQuery(sqlQuery);
  //Validate status of the query execution
  const execution = await validateStatus(queryExecutionId);
  //Get the s3 location of the results
  const s3Location = getAthenaResultsBucketAndPath(execution.QueryExecution.ResultConfiguration.OutputLocation);
  //Start the dataloading
  const jobId = await initLoading(s3Location);
  //Delete the cta table created
  await deleteAthenaTable(ctaTable);
  console.log("jobId: ", jobId);
};

const validateStatus = async (queryExecutionId) => {
  let execute = true;
  while (execute) {
    await sleep(200);
    const execution = await AthenaDAO.getQueryExecution(queryExecutionId);
    switch (execution.QueryExecution.Status.State) {
      case "SUCCEEDED":
        execute = false;
        return execution;
      case "FAILED":
        execute = false;
        throw new Error(execution.QueryExecution.Status.StateChangeReason);
      case "QUEUED":
      case "RUNNING":
        break;
      default:
        break;
    }
  }
};

const sleep = (time) => {
  return new Promise((resolve) => {
    time = parseInt(time) || 500;
    setTimeout(() => {
      return resolve();
    }, time);
  });
};

const getAthenaResultsBucketAndPath = (sAthenaOutputLocation) => {
  const { host, pathname } = parseUrl(sAthenaOutputLocation);
  return { s3Bucket: host, s3Path: pathname.substr(1, pathname.length - 1) };
};

//Init dataloading
const initLoading = async (s3Location) => {
  return new Promise((resolve, reject) => {
    const data = {
      datasetId: `ds`,
      metadataId: metadataId,
      datasources: [
        {
          datasourceId: `dsource`,
          indexName: metadataId,
          dataConnection: {
            appid: `appid`,
            connectorid: `connector`,
            connectorType: "FILE_UPLOAD",
            name: "JSON File Connector",
            s3Bucket: s3Location.s3Bucket,
            s3Path: s3Location.s3Path,
            contentType: "athena",
          },
        },
      ],
    };

    var options = {
      hostname: qrveyPostdataUrl,
      path: "/Prod/dataload/init",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": qrveyApiKey,
      },
      method: "POST",
      port: 443,
    };

    var req = https.request(options, function (res) {
      var chunks = [];
      res.on("data", (chunk) => {
        chunks.push(chunk.toString());
      });
      res.on("end", () => {
        return resolve(chunks);
      });
      res.on("error", (error) => {
        console.log("error: ", error);
        return reject(error);
      });
    });
    var postData = JSON.stringify(data);
    req.write(postData);
    req.end();
  });
};

const deleteAthenaTable = async (table) => {
  const deleteQuery = `DROP TABLE ${table}`;
  const result = await AthenaDAO.executeQuery(deleteQuery);
  return result;
};

run();
