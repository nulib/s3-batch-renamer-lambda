const AWS = require("aws-sdk");
const elasticsearchEndpoint = process.env.elasticsearchEndpoint;
const path = require("path");
const region = process.env.region;
const index = process.env.indexName;
const deleteOriginals = process.env.deleteOriginals;

async function makeRequest(sha256) {
  return new Promise((resolve, _reject) => {
    const endpoint = new AWS.Endpoint(elasticsearchEndpoint);
    const request = new AWS.HttpRequest(endpoint, region);

    const document = {
      _source: ["id"],
      size: 1000,
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

async function fetchIds(sha256) {
  let request = await makeRequest(sha256);
  let response = await awsFetch(request);

  let doc = JSON.parse(response);

  return doc.hits.hits.map((hit) => hit._source.id);
}

async function deleteOriginal(sourceBucket, s3Key) {
  const S3 = new AWS.S3();

  if (deleteOriginals) {
    try {
      await S3.deleteObject({ Bucket: sourceBucket, Key: s3Key }).promise();
    } catch (err) {
      console.log(err, err.stack);
      throw err;
    }
  }
}

exports.handler = async function (event, _context) {
  const taskId = event["tasks"][0]["taskId"];
  const invocationId = event["invocationId"];
  const invocationSchemaVersion = event["invocationSchemaVersion"];
  const s3Key = event["tasks"][0]["s3Key"];
  const s3BucketArn = event["tasks"][0]["s3BucketArn"];
  const S3 = new AWS.S3();
  const pieces = s3BucketArn.split(":");
  const sourceBucket = pieces[pieces.length - 1];
  let resultCode = "Succeeded";
  let resultString = "";

  console.log("event:", JSON.stringify(event));

  try {
    const sha256 = path.basename(s3Key);
    const fileSetIds = await fetchIds(sha256);

    if (fileSetIds.length == 0) {
      throw `Error: no file set found in Elasticsearch for key ${sha256}`;
    }

    const objectInfo = await S3.headObject({
      Bucket: sourceBucket,
      Key: s3Key,
    }).promise();

    console.log("objectInfo", objectInfo);

    if (
      typeof objectInfo.Metadata.sha1 === "undefined" ||
      typeof objectInfo.Metadata.sha256 === "undefined"
    ) {
      throw `Error: no checksums found in metadata for object ${s3Key}`;
    }

    const tags = `computed-sha1=${objectInfo.Metadata.sha1}&computed-sha256=${objectInfo.Metadata.sha256}`;

    var successes = 0;

    const newKeys = await Promise.all(
      fileSetIds.map(async (fileSetId) => {
        const newKey = fileSetId
          .slice(0, 8)
          .match(/.{2}/g)
          .join("/")
          .concat("/", fileSetId);

        const params = {
          Bucket: sourceBucket,
          CopySource: `/${sourceBucket}/${s3Key}`,
          Key: newKey,
          Tagging: tags,
          TaggingDirective: "REPLACE",
        };

        console.log("params:", JSON.stringify(params));

        try {
          await S3.copyObject(params).promise();
          successes += 1;
        } catch (err) {
          console.log(err, err.stack);
        }

        return newKey;
      })
    );

    resultString = newKeys.join(",");
    console.log("successes", successes);
    console.log("fileSetIds.length", fileSetIds.length);
    if (successes === fileSetIds.length) {
      await deleteOriginal(sourceBucket, s3Key);
    } else {
      resultCode = "PermanentFailure";
    }
  } catch (e) {
    console.log(e);
    resultCode = "PermanentFailure";
    resultString = e.hasOwnProperty("code") ? e.code : e;
  }

  let returnResult = {
    invocationSchemaVersion: invocationSchemaVersion,
    treatMissingKeysAs: "PermanentFailure",
    invocationId: invocationId,
    results: [
      {
        taskId: taskId,
        resultCode: resultCode,
        resultString: resultString,
      },
    ],
  };

  return returnResult;
};
