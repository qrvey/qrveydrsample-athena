/**
 * Qrvey 2020
 */

const AWS = require("aws-sdk");
const config = require("./config.json");

// Set the region
AWS.config.update({ region: config.AWS_REGION, accessKeyId: config.ACCESS_KEY_ID, secretAccessKey: config.SECRET_ACCESS_KEY });

//prepare client
const athena = new AWS.Athena();
/**
 *
 *
 */
class AthenaDAO {
  /**
   *  Execute a query on AWS Athena Service
   *
   * @static
   * @param {String} query
   * @returns {String} QueryExecutionId
   * @memberof AthenaDAO
   */
  static async executeQuery(query) {
    const params = {
      QueryString: query,
      QueryExecutionContext: {
        Database: config.ATHENADATABASE,
      },
      ResultConfiguration: {
        EncryptionConfiguration: {
          EncryptionOption: config.ATHENENCRYPTIONOPTION.SSE_S3,
        },
        OutputLocation: `s3://${config.DATA_RESULTS_BUCKET}/athenaresult/`,
      },
    };

    //Execute query on AWS athena service
    const result = await athena.startQueryExecution(params).promise();
    return result.QueryExecutionId;
  }

  /**
   *
   *
   * @static
   * @param {String} QueryExecutionId
   * @returns {Object} Return an object that contains query's status
   * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Athena.html#getQueryExecution-property
   * @memberof AthenaDAO
   */
  static async getQueryExecution(QueryExecutionId) {
    try {
      const params = {
        QueryExecutionId,
      };
      return await athena
        .getQueryExecution(params)
        .promise()
        .catch(function (err) {
          throwError(new Error(err));
        });
    } catch (error) {
      throwError("Athena getQueryExecution error. QueryExecutionID:" + QueryExecutionId, error);
    }
  }
}

module.exports = AthenaDAO;
