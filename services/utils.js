
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



module.exports = {
    ResponseWrapper,
    bindDataToResp,
    JOB_STATUS
}