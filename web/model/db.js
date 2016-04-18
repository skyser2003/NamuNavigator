'use strict';

var fs = require('fs');
var mysql = require('mysql');

class DB {
	init() {
		var self = this;

		if(self.initialized == true) {
			return;
		}

		var dbInfoFilename = '../db/dbinfo.json';

		if(fs.existsSync(dbInfoFilename) == false) {
			console.error(dbInfoFilename + ' doesn\'t exist.');
			return;
		}

		var dbInfoFile = fs.readFileSync(dbInfoFilename);
		var dbInfo = JSON.parse(dbInfoFile);
		
		self.connection = mysql.createConnection({
			host : dbInfo.host,
			port : dbInfo.port,
			user : dbInfo.id,
			password : dbInfo.pw,
			database : dbInfo.db
		});

		var connectErrorHandler = function(err) {
			if(!err) {
				return;
			}

			if(err.code === 'PROTOCOL_CONNECTION_LOST') {
				self.connection.connect(connectErrorHandler);
			}
		};

		self.connection.connect(connectErrorHandler);
		self.initialized = true;
	}

	query(query, callback) {
		var self = this;

		self.connection.query(query, function(err, data) {
			if(err) throw err;
			
			if(callback) {
				callback(data);
			}
		});		
	}
};

module.exports = DB;
