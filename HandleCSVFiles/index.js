const utility = require('./util');
const parse = require('csv-parse');
const fs = require('fs');
const {v4: uuidv4} = require('uuid');
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
    // TODO implement
    console.log({event: JSON.stringify(event)});
    let records = event.Records;
    await Promise.all(
        records.map(record => {
            let fileEventHandler = new HandleFileEvent(s3);
            return fileEventHandler.handleFileEvent(record);
        })
    );

    return response;
};

class HandleFileEvent {
    constructor(s3) {
        this.s3 = s3;
        this.parser = parse({
            delimiter: ','
        });// Create the parser
        this.maxRowCount = process.env.MAX_ROW_COUNT || 2500;
    }

    async handleFileEvent(record) {
        try{
            let self = this;
            let event = JSON.parse(record.body);
            let metadata = event.Metadata;
            let operation = metadata.operation;
            await self[operation](event);
            console.log({
                message: `Operation: ${operation} completed successfully`,
                metadata: JSON.stringify(metadata)
            });
            return Promise.resolve();
        }
        catch(error) {
            console.error({
                message: `Error while executing handleFileEvents`,
                error
            });
            return Promise.resolve(error); // lets not stop due to single record failure
        }
    }

    async transform(event) {
        try {
            let Bucket = event.bucket;
            let Key = event.key;
    
            let {mapping, outputformat} = event.Metadata;
            outputformat = outputformat.trim().toUpperCase();

            await this[`transformTo${outputformat}`](event, Bucket, Key);

            console.log({
                message: `Successfully transformed csv file`,
                Bucket,
                Key,
                outputformat,
                mapping
            });
            return Promise.resolve();
        }
        catch(error) {
            console.error({
                message: `Error while performing transformation`,
                error
            });
            return Promise.reject(error);
        }
    }

    async transformToCSV(event, Bucket, Key) {
        try {
            return new Promise(async (resolve, reject) => {
                let self = this;
                let rowCounter = 0;
                let totalRowCounter = 0;
                let originalHeaders = [];
                let headerMappings = {};
                let {writeStream, fileName} = await this.createTempFileWriteStream('csv');
                let headerSequence = event.Metadata.sequence.split(',');
                const totalOutputHeaders = headerSequence.length;
    
                event.Metadata.mapping.split(',').forEach(mappedPair => {
                    let headers = mappedPair.split(':');
                    headerMappings[headers[1]] = headers[0]; // reverse mapping with current file headers
                });

                let s3ReadStream = await utility.getS3ObjectStream(this.s3, Bucket, Key);
                s3ReadStream.pipe(this.parser);
    
                s3ReadStream.on('finish', () => {
                    console.log(`Reading from S3 read stream finished`),
                    Bucket,
                    Key
                });

                writeStream.on('error', async error => {
                    console.error({message: `Error occurred while writing to tmp directory`, error});
                    writeStream.close();
                    await this.removeTempFile(fileName, 'csv');
                });
    
                let outputRow = ``;
                self.parser.on('readable', async () => {
                    let record;
                    let row = {};
                    while (record = self.parser.read()) {
                        if (originalHeaders.length === 0) {
                            originalHeaders = record;
                            writeStream.write(`${event.Metadata.sequence}\n`); // writing output headers
                        }
                        else {
                            originalHeaders.forEach((header, index) => {
                                row[header] = record[index] ? record[index] : '';
                            });
                            rowCounter++;
                            totalRowCounter++;
                            headerSequence.forEach((header, index) => {
                                outputRow += `${row[headerMappings[header]]}`;
                                if(index !== totalOutputHeaders - 1)
                                    outputRow +=',';
                            });
                            outputRow += `\n`;
                            // self.parser.pause();
                            // writeStream.write(outputRow);
                            // self.parser.resume();
                        }
                        if (self.maxRowCount === rowCounter) {
                            console.log(`max row count reached: ${totalRowCounter}, pausing stream`);
                            self.parser.pause();
                            writeStream.write(outputRow);
                            outputRow = ``;
                            rowCounter = 0;
                            self.parser.resume();
                            console.log(`resuming paused stream: ${totalRowCounter}`);
                        }
                    }

                });

                self.parser.on('end', async () => {
                    writeStream.write(outputRow); // Write remaining
                    console.log({message: `File parsed successfully by csv-parse for total rows: ${totalRowCounter}`});
                    writeStream.close();
                    let uploadResponse = await utility.uploadObjectToS3(self.s3, fileName, 'csv', event.Metadata);
                    await this.removeTempFile(fileName, 'csv');
                    return resolve(uploadResponse);
                });
    
                self.parser.on('error', async error => {
                    console.error({message: `Error encountered inside csv-parse`, error});
                    writeStream.close();
                    await this.removeTempFile(fileName, 'csv');
                    return reject(error);
                });
            });
        }
        catch(error) {
            console.error({
                message: `Error while transforming data to csv`,
                Bucket,
                Key
            });
            return Promise.reject(error);
        }
    }

    async transformToJSON(event, Bucket, Key) {
        try {
            let self = this;
            return new Promise(async (resolve, reject) => {
                let originalHeaders = [];
                let reverseKeyMappings = {};
                let keyMappings = event.Metadata.mapping.split(',');
                let keySequence = event.Metadata.sequence.split(',');
                let totalKeySequence = keySequence.length;
                let rowCounter = 0;
                let totalRowCounter = 0;
                let {writeStream, fileName} = await this.createTempFileWriteStream('json');
    
                keyMappings.forEach(mappedPair => {
                    let mappedKeys = mappedPair.split(':');
                    reverseKeyMappings[mappedKeys[1]] = mappedKeys[0];
                });
    
                let s3ReadStream = await utility.getS3ObjectStream(this.s3, Bucket, Key);
                s3ReadStream.pipe(this.parser);
    
                s3ReadStream.on('finish', () => {
                    console.log(`Reading from S3 read stream finished`),
                        Bucket,
                        Key
                });
    
                writeStream.on('error', async error => {
                    console.error({ message: `Error occurred while writing to tmp directory`, error });
                    writeStream.close();
                    await this.removeTempFile(fileName, 'json');
                });
    
                let outputRow = ``;
                self.parser.on('readable', async () => {
                    let record;
                    let row = {};
                
                    while (record = self.parser.read()) {
                        if (originalHeaders.length === 0) {
                            originalHeaders = record;
                            writeStream.write(`{"items": [\n`);
                        }
                        else {
                            originalHeaders.forEach((header, index) => {
                                row[header] = record[index] ? record[index] : '';
                            });
                            rowCounter++;
                            totalRowCounter++;

                            outputRow += "{";
                            keySequence.forEach((header, index) => {
                                outputRow += `"${header}": "${row[reverseKeyMappings[header]]}"`;
                                if ((totalKeySequence - 1) !== index)
                                    outputRow += ',';
                            });
                            outputRow += `},\n`;
                            // self.parser.pause();
                            // writeStream.write(outputRow);
                            // self.parser.pause();
                        }
                        if (self.maxRowCount === rowCounter) {
                            console.log(`max row count reached: ${totalRowCounter}, pausing stream`);
                            self.parser.pause();
                            writeStream.write(outputRow);
                            outputRow = ``;
                            rowCounter = 0;
                            self.parser.resume();
                            console.log(`resuming paused stream: ${totalRowCounter}`);
                        }
                    }
                });
    
                self.parser.on('end', async () => {
                    writeStream.write(outputRow); // Write remaining 
                    console.log({message: `File parsed successfully by csv-parse for total rows: ${totalRowCounter}`});
                    writeStream.write(`]}\n`);
                    writeStream.close();
                    let uploadResponse = await utility.uploadObjectToS3(self.s3, fileName, 'json', event.Metadata);
                    await this.removeTempFile(fileName, 'json');
                    return resolve(uploadResponse);
                });
    
                self.parser.on('error', async error => {
                    console.error(`Error encountered inside csv-parse`);
                    writeStream.close();
                    await s3ReadStream.close();
                    await this.removeTempFile(fileName, 'csv');
                    return reject(error);
                });
            });
        }
        catch(error) {
            console.error({
                message: `Error while transforming data to json`,
                Bucket,
                Key
            });
            return Promise.reject(error);
        }
    }

    async createTempFileWriteStream(extension) {
        try {
            let fileName = uuidv4();
            let writeStream = fs.createWriteStream(`/tmp/${fileName}.${extension}`);
            console.log({message: `Created a write stream inside /tmp directory`, fileName: `${fileName}.${extension}`});
            return {writeStream, fileName};
        }
        catch(error) {
            console.error({
                message: `Error while creating a write stream`,
                error
            });
            return Promise.reject(error);
        }
    }

    async delay(ms) {
        return new Promise(resolve => {
            return setTimeout(() => resolve(), ms);
        });
    }

    async removeTempFile(fileName, extension) {
        try{
            let destinationFilePath = `/tmp/${fileName}.${extension}`;
            if (fs.existsSync(destinationFilePath)) {
                fs.unlinkSync(destinationFilePath);
                console.log({
                    message: `temp file unlinked`,
                    destinationFilePath
                });
            }
        }
        catch(error) {
            console.error({
                message: `Error while removing tmp file`,
                fileName: `${fileName}.${extension}`,
                error
            });
            return Promise.resolve(error);
        }
    }
}
