const AWS = require('aws-sdk');
const lib = require('lib');

const env = process.env.ENV;
// Set the region 
AWS.config.update({region: 'us-east-1'});

// Create DynamoDB service object
const dynamoClient = new AWS.DynamoDB.DocumentClient();

const parseSQSEvent = (event) => {
    let result = [];
    let records = event.Records;
    records.forEach(record => {
        let jsonBody = JSON.parse(record.body);
        let s3Event = jsonBody.Records[0];
        console.log({messageId: record.messageId, s3Event: JSON.stringify(s3Event)});
        result.push(s3Event);
    });
    return result;
};

const fetchObjectMetaData = async (s3, Bucket, Key) => {
    try{
        let params = {
            Bucket,
            Key
        };
        let metadata = await s3.headObject(params).promise();
        console.log({message: 'Fetched metadata', params, metadata});
        return metadata;
    }
    catch(error){
        console.error({message: 'Error while fetching metadata', Bucket, Key, error});
        return Promise.reject(error);
    }
}

const prepareQueryToFetchConnectionIds = async (metadataArray) => {
    try {
        let params = {
            RequestItems: {
                userConnections: {
                    Keys: []
                }
            }
        };
        let userJobMap = {};
        
        await Promise.all(
            metadataArray.map(metadata => {
                const {jobid, userid, bucket, key} = metadata.Metadata;
                // Prepare userJobMap
                if (userJobMap.hasOwnProperty(userid)) {
                    userJobMap[userid].push({jobid, bucket, key});
                }
                else {
                    userJobMap[userid] = [{jobid, bucket, key}];
                }
                // Prepare dynamoDb Query to fetch user connections
                params.RequestItems.userConnections.Keys.push({
                    userId:  userid
                });
            })
        );
        console.log({
            message: `Prepared dynamodb query`,
            query: JSON.stringify(params),
            userJobMap: JSON.stringify(userJobMap)
        });
        return {params, userJobMap};
    }
    catch(error) {
        console.error({
            message: 'Error while preparing dynamodb query',
            error
        });
        return Promise.reject(error);
    }
}

const fetchUserConnections = async (params) => {
    try {
        let response = await dynamoClient.batchGet(params).promise();
        console.log({
            message: 'dynamoDb response while fetching connection ids',
            params: JSON.stringify(params),
            response: JSON.stringify(response)
        });
        return response;
    }
    catch(error) {
        console.error({
            message: 'Error while fetching user connection ids',
            error
        });
        return Promise.reject(error);
    }
}

const getUserConnectionItems = async (dynamoResponse) => {
    try{
        let response = dynamoResponse.Responses;
        if (!response.hasOwnProperty('userConnections')) {
            console.error({
                message: 'Error while fetching userConnections from dynamoDb',
                response: JSON.stringify(response)
            });
            return Promise.reject(new Error('userConnections not found'));
        }
        let userConnectionItems = response.userConnections;
        return userConnectionItems;
    }
    catch(error) {
        console.log({
            message: 'Error while parsing connection items',
            error
        });
        return Promise.reject(error);
    }
}

const sendNotifications = async (userConnectionItems, userJobMap) => {
    try{
        console.log({
            message: 'inside sendNotifications',
            userConnectionItems
        });
        const notificationUrl = `${process.env.API_GATEWAY_WEBSOCKET_URL}`;
        await Promise.all(
            userConnectionItems.map(async userConnection => {
                try {
                    const url = `${notificationUrl}/${userConnection.connectionId}`;
                    let notificationPayload = {
                        status: 'complete',
                        jobs: userJobMap[userConnection.userId],
                    };
                    const options = {
                        host: process.env.API_GATEWAY_WEBSOCKET_HOST,
                        path: `/${env}${process.env.API_GATEWAY_WEBSOCKET_PATH}/${userConnection.connectionId}`,
                        method: 'POST',
                        region: 'us-east-1',
                        service: 'execute-api',
                        body: JSON.stringify(notificationPayload),
                        contentType: 'text/plain'
                    };
                    let callResponse = await lib.makeSignedAWSRequest(options);
                    console.log({
                        message: 'Sent notification to',
                        notificationPayload: JSON.stringify(notificationPayload),
                        callResponse
                    });
                }
                catch(error) {
                    console.error({message: 'Error in sending notification', error});
                    return Promise.resolve(); // let us not fail due to single failure
                }
            })    
        );
    }
    catch(error) {
        console.error({
            message: 'Error while sending notification',
            error
        });
        return Promise.reject(error);
    }
}


module.exports = {
    parseSQSEvent,
    fetchObjectMetaData,
    prepareQueryToFetchConnectionIds,
    fetchUserConnections,
    sendNotifications,
    getUserConnectionItems
};