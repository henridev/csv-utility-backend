const utility = require('./util');
let AWS = require('aws-sdk');

AWS.config.update({
    accessKeyId: process.env.IAM_ACCESS_KEY_ID,
    secretAccessKey: process.env.IAM_SECRET_KEY,
    region: 'us-east-1'
});

const s3 = new AWS.S3();

const response = {
    statusCode: 200,
    body: JSON.stringify('Hello from Lambda!'),
};

exports.handler = async (event) => {
    try {
        console.log({event: JSON.stringify(event)});
        let sqsEvents = utility.parseSQSEvent(event);
        let s3MetaDataArray = await Promise.all(
            sqsEvents.map(s3Event => {
                return fetchMetaData(s3, s3Event);
            })
        );
        
        let {params = null, userJobMap = null} = await utility.prepareQueryToFetchConnectionIds(s3MetaDataArray);
        if (!params || !userJobMap) {
            response.statusCode = 500;
            response.body = 'Could not generate query to fetch user connections';
            return response;
        }
        console.log(`Fetching user connections`);
        let dynamoResponse = await utility.fetchUserConnections(params);
        let connectionItems = await utility.getUserConnectionItems(dynamoResponse);
        console.log('Done fetching, now notifying');
        console.log({
            message: 'Passing connectionItems',
            connectionItems
        });
        await utility.sendNotifications(connectionItems, userJobMap);
        
        return response;
    }
    catch(error) {
        console.error({
            message: 'Error while notifying clients',
            error
        });
        response.statusCode = 500;
        response.body = JSON.stringify(error);
        return response;
    }
    // TODO implement
};

async function fetchMetaData(s3, s3Event) {
    try{
        let {object, bucket} = s3Event.s3;
        let objectSize = object.size;
        let metaData = await utility.fetchObjectMetaData(s3, bucket.name, object.key);
        metaData.Metadata.bucket = bucket.name;
        metaData.Metadata.key = object.key;
        metaData.Metadata.objectSize = objectSize;
        return metaData;
    }
    catch(error) {
        return Promise.reject(error);
    }
}
