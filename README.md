# CSV utility using AWS serverless
This is a dummy project that uses AWS serverless architecture to submit `csv` files which is then processed based on the metadata sent along with the file. This is `asynchronous event driven` processing setup.


 Link to UI repository: [csv-utility-ui](https://github.com/yogesh8177/csv-utility-ui)

# Architecture diagram
![Architecture diagram](docs/diagrams/csv-utility-architecture.svg)

Please note that the numbers above represent the sequence of operations.

1. User requests a signed url to upload to S3.
    - (1.1) User establishes websocket connection with `userId` (Auto generated on frontend itself).
    - (1.2) Websocket Api gateway calls lambda registered to handle connection event.
    - (1.3) Store the `userId - connectionId` inside `dynamoDb`.

1. Signed Url is returned to client which grants the client permission to upload file to `S3`.
1. User uploads csv file along with some metadata.
1. Once file upload completes, `s3 emits` file upload event and passes this event job to `SQS queue`.
1. `ParseCSVUploads` then fetches metadata of a given `s3 object` and determines the next course of action to be performed on/with this file. It then generates another event and passes on to file operation `SQS queue`.
1. Job for handling files is stored inside `File operation queue`.
1. `HandleCSVFiles` then picks up the job from file operations queue and performs the respective operation with given file. In this case, we transform files (csv and json) and output the transformed file into `s3` inside a dedicated bucket for output files. Once operation completes, we generate a success notification message and pass it on to `notification queue`.
1. `Notification queue` stores notification message.
1. `NotifyClient` lamda fetches notification message.
1. `NotifyClient` then fetches user connection from dynamoDB and makes an api call to `websocket api gateway`.
1. `NotifyClient` calls websocket api gateway to send message to connected client.
1. `Websocket api gateway` notifies the client about operation status.
1. `Client` requests a signed url to download the transformed file.
1. `Client` downloads transformed file based on signed url from `s3`.

# AWS Components used
- S3
- SQS Queue
- AWS Lambda
- HTTP API Gateway
- Websockets API Gateway

# To do
- [ ] DLQs for failed messages.
- [ ] Error notifications to clients
- [ ] Code refactoring and cleanup
- [ ] Do not stringify `Error` object :P
- [ ] Setup different environments (test, staging and production)