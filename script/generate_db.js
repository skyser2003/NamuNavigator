var fs = require('fs');
var JSONStream = require('JSONStream');
var es = require('event-stream');
var mysql = require('mysql');
var util = require('util');

var filename = '../db/namuwiki.json';
var dbInfoFilename = '../db/dbinfo.json';

if(fs.existsSync(filename) == false) {
	console.error(filename + ' doesn\'t exist.');
	return;
}

if(fs.existsSync(dbInfoFilename) == false) {
	console.error(dbInfoFilename + ' doesn\'t exist.');
	return;
}

var dbInfoFile = fs.readFileSync(dbInfoFilename);
var dbInfo = JSON.parse(dbInfoFile);

var pool = mysql.createPool({
	connectionLimit : 100,
	host : dbInfo.host,
	port : dbInfo.port,
	user : dbInfo.id,
	password : dbInfo.pw,
	database : dbInfo.db
});

var stream = fs.createReadStream(filename, {encoding: 'utf8'});
var parser = JSONStream.parse('*');

var ended = false;

var rowCount = 0;
var dataErrorCount = 0;
var sqlErrorCount = 0;
var totalCount = 0;

var test = false;
var testCount = 100;

console.log('Parsing JSON file and inserting into database.');

var dbSave = es.mapSync(function(data) {
	if(test == true) {
		if(testCount <= 0) {
			parser.emit('end');
			return;
		}

		--testCount;
	}

	++totalCount;

	if(data.title === undefined || data.text === undefined || data.namespace === undefined) {
		++dataErrorCount;
	}
	else {
		pool.query('INSERT INTO `page` (`title`, `text`, `namespace`) VALUES(?, ?, ?)', [data.title, data.text, data.namespace], function(err, data) {
			if(err) {
				console.log(err);
				++sqlErrorCount;
			}
			else {
				++rowCount;
			};

			if(ended == true && totalCount == (sqlErrorCount + rowCount)) {
				pool.end(function(err) {
					console.log('Total page : ' + rowCount);
					console.log('Data error : ' + dataErrorCount);
					console.log('SQL error : ' + sqlErrorCount);
				});
			}
		});
	}
}).on('end', function() {
	ended = true;
	console.log('end');

	stream.emit('end');
});

stream.pipe(parser).pipe(dbSave);
