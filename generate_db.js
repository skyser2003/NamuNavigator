var fs = require('fs');
var JSONStream = require('JSONStream');
var es = require('event-stream');
var mysql = require('mysql');
var util = require('util');

var connection = mysql.createConnection({
	host : "localhost",
	port : 3306,
	user : 'root',
	password : 'dnaxxwmf',
	database : 'namuwiki'
});

connection.connect(function(err){
	if(err) throw err;
});

var stream = fs.createReadStream('data/namuwiki.json', {encoding: 'utf8'});
var parser = JSONStream.parse('*');

var i = 0;

stream.pipe(parser).pipe(es.mapSync(function(data){
	connection.query('INSERT INTO `page` (`title`, `text`, `namespace`) VALUES(?, ?, ?)', [data.title, data.text, data.namespace]);
	++i;
}).on('end', function() {
	console.log("Total pages : " + i);
	connection.end();
}));
