let AWS = require('aws-sdk');
AWS.config.update({
    accessKeyId: process.env.IAM_ACCESS_KEY_ID,
    secretAccessKey: process.env.IAM_SECRET_KEY,
    region: 'us-east-1',
    signatureVersion: 'v4'
});
let s3 = new AWS.S3();

const response = {
    statusCode: 200
};

exports.handler = async (event) => {
    console.log(`event: ${JSON.stringify(event)}`);
    const eventBody = JSON.parse(event.body);
    let bucketName = process.env.CSV_UPLOAD_BUCKET || null;
    let expires    = process.env.SIGNED_URL_EXPIRATION || 600;

    if (!bucketName) {
        let errorMessage = 'Could not find bucketName to get signedUrl';
        console.error({errorMessage, bucketName});
        response.statusCode = 500;
        response.body = errorMessage;

        return response;
    }
    if (eventBody.hasOwnProperty('metadata')) {
        console.log({message: `Generating signed url for bucketName: ${bucketName}`});
        let params = {
            Bucket: bucketName,
            Expires: parseInt(expires),
            Fields: {
                Key: `${eventBody.metadata.userId}/csv_upload_${new Date().getTime()}.csv`
            },
            Conditions: [
                ['content-length-range', 10, 40000000] // 40MB
            ]
        };
        addMetaDataHeadersToConditions(params, eventBody.metadata);
        let signedUrl = await new Promise((resolve, reject) => {
            s3.createPresignedPost(params, (err, data) => {
                if (err) return reject(err);
                return resolve(data);
            });
        });
        console.log({signedUrl});
        response.body = JSON.stringify(signedUrl);
    }
    else if(eventBody.hasOwnProperty('bucketData')) {
        const {Bucket, Key} = eventBody.bucketData;
        let params = {
            Bucket,
            Key, 
            Expires: parseInt(expires)
        };
        const keyContents = Key.split('/');
        const fileName = `${keyContents[keyContents.length - 1]}`;
        const signedUrl = await s3.getSignedUrlPromise('getObject', params);
        
        response.body = JSON.stringify({url: signedUrl});
    }
    else {
        response.statusCode = 422;
        response.body = `metadata is missing from event`;
        console.error({errorMessage: `metadata is missing from event`, event: JSON.stringify(event)});
    }
    return response;
};

function addMetaDataHeadersToConditions (params, metadata) {
    Object.keys(metadata).forEach(header => {
        params.Conditions.push({[`x-amz-meta-${header.toLowerCase()}`]: metadata[header]});
        params['Fields'][`x-amz-meta-${header.toLowerCase()}`] = metadata[header];
    });

    console.log({
        message: `meta headers added to conditions`,
        params: JSON.stringify(params)
    });
}
