const AWS = require("aws-sdk");
const elasticsearchEndpoint = process.env.elasticsearchEndpoint;
const path = require("path");
const region = process.env.region;
const index = process.env.indexName;

async function makeRequest(sha256) {
  return new Promise((resolve, _reject) => {
    const endpoint = new AWS.Endpoint(elasticsearchEndpoint);
    const request = new AWS.HttpRequest(endpoint, region);

    const document = {
      _source: ["id"],
      size: 1,
      query: {
        bool: {
          must: [
            { match: { "model.name.keyword": "FileSet" } },
            { match: { "digests.sha256.keyword": sha256 } },
          ],
        },
      },
    };

    request.method = "POST";
    request.path += index + "/_search";
    request.body = JSON.stringify(document);
    request.headers["host"] = elasticsearchEndpoint;
    request.headers["Content-Type"] = "application/json";
    request.headers["Content-Length"] = Buffer.byteLength(request.body);

    let chain = new AWS.CredentialProviderChain();
    chain.resolve((err, credentials) => {
      if (err) {
        console.error("Returning unsigned request: ", err);
      } else {
        var signer = new AWS.Signers.V4(request, "es");
        signer.addAuthorization(credentials, new Date());
      }
      resolve(request);
    });
  });
}

async function awsFetch(request) {
  return new Promise((resolve, reject) => {
    var client = new AWS.HttpClient();
    client.handleRequest(
      request,
      null,
      function (response) {
        let responseBody = "";
        response.on("data", function (chunk) {
          responseBody += chunk;
        });
        response.on("end", function (chunk) {
          resolve(responseBody);
        });
      },
      function (error) {
        console.error("Error: " + error);
      }
    );
  });
}

async function fetchId(sha256) {
  let request = await makeRequest(sha256);
  let response = await awsFetch(request);

  let doc = JSON.parse(response);

  console.log("response: ", response);

  return doc?.hits?.hits[0]?._source?.id;
}

exports.handler = async function (event, _context, callback) {
  const taskId = event["tasks"][0]["taskId"];
  const invocationId = event["invocationId"];
  const invocationSchemaVersion = event["invocationSchemaVersion"];
  const s3Key = event["tasks"][0]["s3Key"];
  const s3BucketArn = event["tasks"][0]["s3BucketArn"];
  const S3 = new AWS.S3();
  const pieces = s3BucketArn.split(":");
  const sourceBucket = pieces[pieces.length - 1];
  var resultCode = "Succeeded";

  console.log("event:", JSON.stringify(event));

  try {
    const sha256 = path.basename(s3Key);
    let fileSetId = await fetchId(sha256);

    const params = {
      Bucket: sourceBucket,
      CopySource: `/${sourceBucket}/${s3Key}`,
      Key: fileSetId
    };

    console.log("params:", JSON.stringify(params));

    if (fileSetId) {
      await S3.copyObject(params).promise();
    } else {
      throw `Error retrieving fileSetID for ${sha256}`;
    }
  } catch (e) {
    console.log(e);
    resultCode = "PermanentFailure";
  }

  let returnResult = {
    invocationSchemaVersion: invocationSchemaVersion,
    treatMissingKeysAs: "PermanentFailure",
    invocationId: invocationId,
    results: [
      {
        taskId: taskId,
        resultCode: resultCode,
        resultString: s3Key,
      },
    ],
  };

  callback(null, returnResult);
};
