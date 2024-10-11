const bcrypt = require('bcrypt');

const APP_PASSWORD = process.env.APP_PASS || null;
const APP_USERNAME = process.env.APP_USERNAME || null;



function login(username = null, password = null) {
    if (!(APP_USERNAME || APP_PASSWORD)) {
        throw new Error('Username or password is not defined')
    }

    if (!(username || password)) {
        throw new Error('Username or password is empty')
    }

    if (username.toLowerCase() === APP_USERNAME.toLowerCase() 
        && bcrypt.compareSync(password, APP_PASSWORD)) {
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