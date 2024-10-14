const bcrypt = require('bcrypt');

const {
    appUsername,
    appPassword
} = require('./../config/default');


function login(username = null, password = null) {
    if (!(appUsername || appPassword)) {
        throw new Error('Username or password is not defined')
    }

    if (!(username || password)) {
        throw new Error('Username or password is empty')
    }

    if (username.toLowerCase() === appUsername?.toLowerCase() 
        && bcrypt.compareSync(password, appPassword)) {
        return true;
    }

    throw new Error('Username or password is wrong')
}



function validateSession() {
    return (req, resp, next) => {       
        if (!req?.session?.user) {
            return resp.redirect('/login');
        }
        return next();
    }
}

module.exports = {
    validateSession,
    login
}