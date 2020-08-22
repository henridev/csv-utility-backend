const AWS = require("aws-sdk");
AWS.config.update({
    region: "us-east-1"
});

const dynamoClient = new AWS.DynamoDB.DocumentClient();

const handlers = {
    connectHandler: onNewConnection,
    disconnectHandler: onDisconnnection,
    sendMessageHandler: onMessage,
    defaultHandler: onDefaultEvent
};

async function onNewConnection(event) {
    try{
        const {connectionId}  = event.requestContext;
        const {userId = null} = event.queryStringParameters;
        if (!userId) {
            console.error(`userId not found, will not update connectionId in dynamodb`);
            return true;
        }
        console.log(`Adding new connection: ${connectionId} to dynamodb for userId: ${userId}`);
        let updateResponse = await dynamoClient.update({
            TableName: 'userConnections',
            Key: {
                userId
            },
            UpdateExpression: `set #connectionId = :connectionId`,
            ExpressionAttributeNames: {'#connectionId' : 'connectionId'},
            ExpressionAttributeValues: {
                ":connectionId": connectionId
            },
            ReturnValues: "ALL_NEW"
        }).promise();
        console.log({
            message: `dynamodb update response`,
            response: updateResponse
        });
        return true;
    }
    catch(error){
        console.error({
            message: `Error while handling new connection`,
            error
        });
        return Promise.reject(error);
    }
}

async function onDisconnnection(event) {
    try{
        console.log({
            message: `Client disconnected`,
            event: JSON.stringify(event)
        });
    }
    catch(error) {
        
    }
}

async function onMessage(event) {
    try{
        
    }
    catch(error) {
        
    }
}
async function onDefaultEvent(event) {
    try{
        console.log({
            message: `Default handler`,
            event: JSON.stringify(event)
        });
    }
    catch(error) {
        
    }
}

const handleEvent = async (event) => {
    const {connectionId = null, appId, routeKey} = event.requestContext;
    const eventHandler = `${routeKey.replace('$', '')}Handler`;
    if (handlers.hasOwnProperty(eventHandler)) {
        return handlers[eventHandler](event);
    }
    else {
        console.error({
            message: `Invalid event handler encountered`,
            routeKey
        });
        return Promise.resolve();
    }
}


module.exports = handleEvent;