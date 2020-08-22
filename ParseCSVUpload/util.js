const {v4: uuidv4} = require('uuid');

const QUEUE_MAPPING = {
    'transform': process.env.FILE_HANDLE_QUEUE
};

let parseSQSEvent = (event) => {
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

let fetchObjectMetaData = async (s3, Bucket, Key) => {
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

let generateS3FileHandleEvents = async (sqs, s3MetaDataArray) => {
    try{
        let sqsBatchPayload = {};
        let response = await Promise.all(
            s3MetaDataArray.map(async item => {
                let fileHandleEvent = Object.assign({}, item); 
                let operation = fileHandleEvent.Metadata.operation;
                const QueueUrl = QUEUE_MAPPING.hasOwnProperty(operation) ? QUEUE_MAPPING[operation] : null;

                if (QueueUrl === null) {
                    let errorObject = {
                        message: `Unexpected operation encountered while generating file handle event`,
                        operation,
                        fileHandleEvent: JSON.stringify(fileHandleEvent)
                    };
                    console.warn(errorObject);
                    return Promise.resolve(errorObject);
                }
                let sqsMessage = {
                    Id: uuidv4(),
                    MessageBody: JSON.stringify(fileHandleEvent)
                };
                // Group sqs events based on operation type
                if (!sqsBatchPayload.hasOwnProperty(operation)) {
                    sqsBatchPayload[operation] = [sqsMessage];
                }
                else {
                    sqsBatchPayload[operation].push(sqsMessage);
                }
                console.log({
                    message: `Successfully generated file handle event message`,
                    sqsMessage: JSON.stringify(sqsMessage),
                    fileHandleEvent: JSON.stringify(fileHandleEvent)
                });
            })
        );

        let finalResponse = await Promise.all(
            Object.keys(sqsBatchPayload).map(async operationType => {
                let params = {
                    Entries: sqsBatchPayload[operationType],
                    QueueUrl: QUEUE_MAPPING[operationType]
                };
                let sqsResponse = await sqs.sendMessageBatch(params).promise();
                console.log({
                    message: `generated file handle event for operation: ${operationType}`,
                    sqsResponse: JSON.stringify(sqsResponse),
                    QueueUrl: QUEUE_MAPPING[operationType]
                });
            })
        );
        return finalResponse;
    }
    catch(error) {
        console.error({
            message: `Error while generating s3 file handle event`, 
            s3MetaDataArray: JSON.stringify(s3MetaDataArray), 
            error
        });
        return Promise.reject(error);
    }
}

module.exports = {
    parseSQSEvent,
    fetchObjectMetaData,
    generateS3FileHandleEvents
};