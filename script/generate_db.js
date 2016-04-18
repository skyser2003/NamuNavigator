var fs = require('fs');
var JSONStream = require('JSONStream');
var es = require('event-stream');
var mysql = require('mysql');
var util = require('util');

var filename = '../db/namuwiki.json';

if(fs.existsSync(filename) == false) {
	console.error(filename + ' doesn\'t exist.');
	return;
}

var pool = mysql.createPool({
	connectionLimit : 100,
	host : 'localhost',
	port : 3306,
	user : 'root',
	password : 'dnaxxwmf',
	database : 'namuwiki'
});

var stream = fs.createReadStream(filename, {encoding: 'utf8'});
var parser = JSONStream.parse('*');

var flag = 0;
var rowCount = 0;
var dataErrorCount = 0;
var sqlErrorCount = 0;
var finishedCount = 0;

console.log('Parsing JSON file and inserting into database.');

stream.pipe(parser).pipe(es.mapSync(function(data) {
	if(data.title === undefined || data.text === undefined || data.namespace === undefined) {
		++dataErrorCount;
	}
	else {
		pool.query('INSERT INTO `page` (`title`, `text`, `namespace`) VALUES(?, ?, ?)', [data.title, data.text, data.namespace], function(err, data) {
			++finishedCount;

			if(flag == 1 && finishedCount == (sqlErrorCount + rowCount)) {
				flag = 0;
				pool.end(function(err) {
					console.log('Total page : ' + rowCount);
					console.log('Data error : ' + dataErrorCount);
					console.log('SQL error : ' + sqlErrorCount);
				});
			}			

			if(err) {
				console.log(err);
				++sqlErrorCount;
				return;
			}

			++rowCount;
		});
	}
}).on('end', function() {
	flag = 1;
}));
