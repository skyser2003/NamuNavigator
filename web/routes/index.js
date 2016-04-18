var express = require('express');
var router = express.Router();
var DB = require('../model/db.js');

var db = new DB();

/* GET home page. */
router.get('/', function(req, res, next) {
	db.init();
	db.query('SELECT * FROM `page` JOIN (SELECT ceil(rand() * (SELECT MAX(`uid`) FROM `page`)) as id) as r2 WHERE `page`.`uid` = `r2`.`id`', function(data) {
		res.render('page', data[0]);
	});

});

module.exports = router;
