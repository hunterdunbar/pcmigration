
const express = require('express');
const router = express.Router();
const { login } = require('../services/security')

router.get('/login', (req, resp) => {
    if (req?.session?.user) {
        return resp.redirect('/');
    }
    resp.render('login')
})

router.post('/login', (req, resp) => {
    const {username, password} = req.body;
    try {
        login(username, password);
        req.session.user = username;
        return resp.redirect('/')
    } catch(e) {
        return resp.render('login', { username, password, errorMessage: e.message });
    }
})


module.exports = router