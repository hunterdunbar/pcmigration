
const express = require('express');
const router = express.Router();


const loginCtrl = require('./login');
const packageExportCtrl = require('./packageExport');

router.use(loginCtrl);
router.use(packageExportCtrl);


router.get('/', async (req, resp) => {
    return resp.redirect('/packageExport')
})

module.exports = router