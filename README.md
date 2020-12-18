# Introduction
This sample to show how to use datarouter to load data from athena database using a query.

# Prerequisites
Before using this sample, please have the following values
1. Metadata URL. Contact help@qrvey.com if you don't have one.
2. Postdata URL. Contact help@qrvey.com if you don't have one.
3. Datarouter API Key

# Installation

1. Install NodeJS from https://nodejs.org (v12 recommended)
2. Run the command to load data 

# Steps

## 1. Create Metadata

Create the metadata specifying the fields and data types.

Example:

```
curl --location --request POST '{{metadataurl}}/v5/metadata?publicConnection=true' \
--header 'x-api-key: {{api-key}}' \
--header 'Content-Type: application/json' \
--data-raw '{
    "MetaDataId": "quick_start_index_name",
    "indexName": "quick_start_index_name",
    "skipOnPartialData": false,
    "dateFormat": "YYYY-MM-DDTHH:mm:ss",
    "columnType": [
        {
            "columnName": "CompanyId",
            "type": "numeric-general"
        },
        {
            "columnName": "Company Name",
            "type": "text-label"
        },
        {
            "columnName": "Foundation Date",
            "type": "date"
        }
    ]
}'
```

## 2. Create/Update a file called "config.json".

In root folder with the following JSON:
```
{
  "metadataId": "", //Use metadata created in the step #1
  "postdataurl": "<postdataurl>", // Example: https://<API ID>.execute-api.<region>.amazonaws.com/Prod/dataload
  "apikey": "<API key>",
  "query":"", //Query to execute in athena database.
  "AWS_REGION":"", //Aws region
  "ACCESS_KEY_ID":"", //Access key of the account where the athena database exists.
  "SECRET_ACCESS_KEY":"", //Secret access key of the account where the athena database exists.
  "ATHENADATABASE":"", //Athena database where to execute query.
  "DATA_RESULTS_BUCKET":"" //S3 bucket to save the result of the query
}
```
# Usage

Load Data: `node index.js -q "select * from table"`