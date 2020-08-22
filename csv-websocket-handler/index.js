const handleEvent = require('./websocketEventHandler');

const response = {
    statusCode: 200,
    body: JSON.stringify('Hello from Lambda!'),
};
exports.handler = async (event) => {
    try {
        console.log({event: JSON.stringify(event)});
        await handleEvent(event);
        console.log('Websocket event handling complete');
        return response;
    }
    catch(error) {
        response.statusCode = 500;
        response.body = JSON.stringify(error);
        return response;
    }
};
