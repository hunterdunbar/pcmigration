
const express = require('express');
const router = express.Router();


const loginCtrl = require('./login');
const packageExportCtrl = require('./packageExport');
const { validateSession } = require('./../services/security');


router.use(loginCtrl);
router.use(validateSession(), packageExportCtrl);


router.get('/', async (req, resp) => {
    return resp.redirect('/packageExport')
})

module.exports = router