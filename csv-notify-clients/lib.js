const crypto = require("crypto");
const axios = require('axios');

const AWS_ACCESS_KEY = process.env.IAM_ACCESS_KEY_ID.trim();
const AWS_SECRET_KEY = process.env.IAM_SECRET_KEY.trim();

const getSignatureKey = (key, dateStamp, regionName, serviceName) => {
    const kDate = crypto.createHmac('sha256', "AWS4" + key).update(dateStamp, 'utf8').digest();
    const kRegion = crypto.createHmac('sha256', kDate).update(regionName, 'utf8').digest();
    const kService = crypto.createHmac('sha256', kRegion).update(serviceName, 'utf8').digest();
    const kSigning = crypto.createHmac("sha256", kService).update('aws4_request', 'utf8').digest();
    return kSigning;
}

const generateAWSDate = () => {
    let d = new Date();

    let year = d.getFullYear();
    let month = d.getMonth() + 1;
    month = month < 10 ? `0${month}` : month;
    let date = d.getDate();
    date = date < 10 ? `0${date}` : date;
    let hours = d.getHours();
    let minutes = d.getMinutes();
    minutes = minutes < 10 ? `0${minutes}` : minutes;
    let seconds = d.getSeconds();
    seconds = seconds < 10 ? `0${seconds}` : seconds;

    let dateStamp = `${year}${month}${date}`;
    //let xAmazonDate = `${year}${month}${date}T${hours}${minutes}${seconds}Z`;
    let xAmazonDate = `${d.toISOString().replace(/[:-]/g, '').split('.')[0]}Z`;

    return {dateStamp, xAmazonDate};
}

const makeSignedAWSRequest = async (options) => {
    try {
        console.log({
            message: 'Inside makeSignedAWSRequest',
            options
        });
        const {method, body, host, path, service, region, contentType} = options;
        const {dateStamp, xAmazonDate} = generateAWSDate();
        let canonicalUri = path.split('/');
        canonicalUri = canonicalUri.map(uri => encodeURIComponent(uri));
        //canonicalUri = canonicalUri.map(uri => encodeURIComponent(uri));
        canonicalUri = canonicalUri.join('/');
        const payloadHash = crypto.createHash('sha256').update(body, 'utf8').digest('hex');
        const canonicalHeaders = `content-type:${contentType}\nhost:${host}\nx-amz-content-sha256:${payloadHash}\nx-amz-date:${xAmazonDate}\n`;
        const canonicalQueryString = '';
        const signedHeaders = 'content-type;host;x-amz-content-sha256;x-amz-date';

        const canonicalRequest = `${method}\n${canonicalUri}\n${canonicalQueryString}\n${canonicalHeaders}\n${signedHeaders}\n${payloadHash}`;
        const canonicalRequestHash = crypto.createHash('sha256').update(canonicalRequest, 'utf8').digest('hex');
        const algorithm = 'AWS4-HMAC-SHA256';
        const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`;

        const stringToSign = `${algorithm}\n${xAmazonDate}\n${credentialScope}\n${canonicalRequestHash}`;
        
        const signingKey = getSignatureKey(AWS_SECRET_KEY, dateStamp, region, service);
        const signature = crypto.createHmac('sha256', signingKey).update(stringToSign, 'utf8').digest('hex');

        const authorizationHeader = `${algorithm} Credential=${AWS_ACCESS_KEY}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

        const headers = {
            'Content-Type': contentType,
            'X-Amz-Date': xAmazonDate,
            'X-Amz-Content-Sha256': payloadHash,
            'Authorization': authorizationHeader,
            'Content-Length': Buffer.byteLength(body)
        };

        console.log({
            message: 'makeSignedAWSRequest headers',
            headers,
            canonicalUri,
            canonicalHeaders,
            canonicalRequest,
            stringToSign
        });

        let response = await axios({
            url: `https://${host}${path}`,
            method,
            data: body,
            headers
        });
        console.log({
            message: 'AWS signed request complete',
            response
        });
        return response;
    }
    catch(e) {
        console.error({
            message: 'Error while making signed AWS request',
            error: typeof e === 'object' && e.hasOwnProperty('response') ? e.response : e
        });
        return Promise.reject(e.response);
    }
}

module.exports = {
    makeSignedAWSRequest
};