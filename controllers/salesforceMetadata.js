const express = require('express');
const router = express.Router();
const { validateSession } = require('./../services/security')

router.post('/generatePackageXml', validateSession(), (req, resp) => {

    const { tableName } = req.body;

    if (!tableName) {
        resp.render('index', { errorMessage : 'Table is not selected' })
    } else {
        resp.render('index', { errorMessage : 'Ok' })
    }
    
    console.log(tableName);

})

module.exports = router