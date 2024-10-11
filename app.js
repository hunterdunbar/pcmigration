const express = require('express');
const enforce = require('express-sslify');
const app = express();
const path = require('path');
const session = require('express-session');

const controllers = require('./controllers');

var SESSION = {
    secret: process.env.SESSION_SECRET,
    cookie: {
        maxAge : 3600000 //
    },
    resave : false,
    saveUninitialized : true
}

if (process.env.NODE_ENV !== 'development') {
    app.use(enforce.HTTPS({ trustProtoHeader : true }))
    app.set('trust proxy', 1) // trust first proxy
    SESSION.cookie.secure = true // serve secure cookies
}

//setup session
app.use(session(SESSION))


//all API in json
app.use(express.json());

//post body
app.use(express.urlencoded({ extended : true }))

//pub pluging setup
app.set('views', './views')
app.set('view engine', 'pug')
app.use(express.static(path.join(__dirname, './public')));

//ui and etc
app.use(controllers);

//handle all unexpected errors
app.use((error, req, res, next) => {
    
    console.error('ERROR:', { error })

    res.status(error?.status || 500);
    res.send(typeof error === 'string' ? error : error.message);
})

module.exports = app;