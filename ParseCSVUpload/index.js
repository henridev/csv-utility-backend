const utility = require('./util');
let AWS = require('aws-sdk');

AWS.config.update({
    accessKeyId: process.env.IAM_ACCESS_KEY_ID,
    secretAccessKey: process.env.IAM_SECRET_KEY,
    region: 'us-east-1'
});

const response = {
    statusCode: 200,
    body: JSON.stringify('Hey')
};

const s3 = new AWS.S3();
const sqs = new AWS.SQS();

exports.handler = async (event) => {
    try{
        console.log({event: JSON.stringify(event)});
        let sqsEvents = utility.parseSQSEvent(event);
        let s3MetaDataArray = await Promise.all(
            sqsEvents.map(s3Event => {
                return fetchMetaData(s3, s3Event);
            })
        );

        let sqsResponses = await utility.generateS3FileHandleEvents(sqs, s3MetaDataArray);
        console.log({message: `Processed ${sqsEvents.length} sqsEvent(s) in this lambda`, sqsResponses: JSON.stringify(sqsResponses)});
        response.body = JSON.stringify(s3MetaDataArray);
        return response;
    }
    catch(error){
        return Promise.reject(error);
    }
};

async function fetchMetaData(s3, s3Event) {
    try{
        let {object, bucket} = s3Event.s3;
        let objectSize = object.size;
        let metaData = await utility.fetchObjectMetaData(s3, bucket.name, object.key);
        metaData.bucket = bucket.name;
        metaData.key = object.key;
        metaData.objectSize = objectSize;
        return metaData;
    }
    catch(error) {
        return Promise.reject(error);
    }
}