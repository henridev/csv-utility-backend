# CSV utility using AWS serverless
This is a dummy project that uses AWS serverless architecture to submit `csv` files which is then processed based on the metadata sent along with the file. This is `asynchronous event driven` processing setup.

# Architecture diagram
![Architecture diagram](docs/diagrams/csv-utility-architecture.svg)

Please note that the numbers above represent the sequence of operations.

# AWS Components used
- S3
- SQS Queue
- AWS Lambda
- HTTP API Gateway
- Websockets API Gateway

# To do
- [ ] DLQs
- [ ] Error notifications to clients
- [ ] Code refactoring and cleanup
- [ ] Do not stringify `Error` object :P