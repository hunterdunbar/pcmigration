const crypto = require('crypto');
class ResponseWrapper {

    isSuccessful = true;
    messages = [];
    data;

    constructor(isSuccessful = true, messages, data) {
        this.isSuccessful = isSuccessful;
        this.messages = messages ? this.messages.concat(messages) : this.messages;
        this.data = JSON.stringify(data)
    }

    static Error(error) {
        if (error instanceof Error) {
            return new ResponseWrapper(false, error.message)
        }
        
        if (typeof error === 'string') {
            return new ResponseWrapper(false, error);
        }
        
        return new ResponseWrapper(false);
    }

    static Success(data, message) {
        return new ResponseWrapper(true, message, data);
    }
}

function bindDataToResp(resp, methodPromise) {
    return methodPromise
        .then(ResponseWrapper.Success)
        .catch(e => {
            console.error('[ERROR]: bindDataToResp: ', e);
            return ResponseWrapper.Error(e)
        })
        .then(resp.send.bind(resp))
}

const JOB_STATUS = {
    Pending : 'Pending',
    Processing : 'Processing',
    Completed : 'Completed',
    Error : 'Error'
}

function hash20base64(str) {
    const hash = crypto.createHash('sha256').update(str).digest('base64');
    return hash.replace(/[^a-zA-Z0-9]/g, '').slice(0, 20);
}


function timeoutPromise(message, timeout = 30000) {
    return new Promise((_, reject) => {
        setTimeout(() => {
            reject(message);
        }, timeout);
    });
}


module.exports = {
    ResponseWrapper,
    bindDataToResp,
    JOB_STATUS,
    hash20base64,
    timeoutPromise
}