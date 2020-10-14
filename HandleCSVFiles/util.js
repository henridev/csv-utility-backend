const fs = require('fs');

const getS3ObjectStream = async (s3, Bucket, Key) => {
    try{
        let params = {
            Bucket,
            Key
        };
        let s3ReadStream = s3.getObject(params).createReadStream();    
        s3ReadStream.on('error', error => {
            console.error({
                message: `S3 object read stream error`,
                error,
                Bucket,
                Key
            });
            s3ReadStream.close();
        });

        console.log({
            message: `Created readstream for s3 object`,
            Bucket,
            Key
        });
        return s3ReadStream;
    }
    catch(error) {
        console.error({
            message: `Error while reading s3 object stream`,
            error: JSON.stringify(error)
        });
        return Promise.reject(error);
    }
};

const uploadObjectToS3 = async (s3, fileName, extension, Metadata) => {
    try{
        const {operation, userid} = Metadata;
        let destinationFileName   = `transformed_${fileName}`;
        let readableStream        = fs.createReadStream(`/tmp/${fileName}.${extension}`);
        let Bucket                = process.env.TRANSFORMED_FILE_BUCKET;
        let Key                   = `${userid}/${operation}/${extension}/${destinationFileName}.${extension}`;
        let params                = {Bucket, Key, Body: readableStream, Metadata};

        let uploadResponse = await s3.upload(params).promise();
        console.log({
            message: `File uploaded to S3 bucket`,
            Bucket,
            Key
        });
        return uploadResponse;
    }
    catch(error) {
        console.error({
            message: `Error while uploading object to s3`,
            fileName: `${fileName}.${extension}`,
            error
        });
        return Promise.resolve(error);
    }
}

module.exports = {
    getS3ObjectStream,
    uploadObjectToS3
};