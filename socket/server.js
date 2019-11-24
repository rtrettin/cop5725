var fs = require('fs');
var https = require('https');
// setup HTTPS web server
var server = https.createServer({
	key: fs.readFileSync('domain.key'), // SSL private key
	cert: fs.readFileSync('domain.crt'), // SSL certificate
	ca: fs.readFileSync('ca.crt') // SSL certificate authority bundle
}),
	io = require('socket.io')(server),
	logger = require('winston');

// setup the console and file loggers
logger.remove(logger.transports.Console);
logger.add(logger.transports.Console, {colorize:true,timestamp:true});
logger.add(logger.transports.File, {
	level: 'info',
	colorize: false,
	timestamp: true,
	filename: 'socket.log',
	maxsize: 104857600,
	maxFiles: 5,
	json: false,
	tailable: true
});
logger.info('Socket > listening on https://199.15.250.44:8000');

// create and manage web sockets communication messages
var connections = {};
io.set('origins', 'https://crustycrab.proxcp.com:443 https://199.15.250.44:4433');
io.on('connection', function(socket) {
	logger.info('Socket > Connected socket ' + socket.id);
	socket.on('addUserConnection', function(data) {
		connections[socket.id] = data;
	});
	socket.on('disconnect', function() {
		logger.info('Socket > Disconnected socket ' + socket.id);
		delete connections[socket.id];
	});
	
	socket.on('Web_ConnectHyracksReq', function(data) {
		if(Object.keys(data).length !== 0 && data.constructor === Object) {
			logger.info('Received Web_ConnectHyracksReq');
			socket.broadcast.emit('Java_ConnectHyracksReq', data);
		}
	});
	socket.on('Java_ConnectHyracksRes', function(data) {
		logger.info('Received Java_ConnectHyracksRes');
		socket.broadcast.emit('Web_ConnectHyracksRes', data);
	});
	
	socket.on('Web_ConnectSQL1Req', function(data) {
		if(Object.keys(data).length !== 0 && data.constructor === Object) {
			logger.info('Received Web_ConnectSQL1Req');
			socket.broadcast.emit('Java_ConnectSQL1Req', data);
		}
	});
	socket.on('Java_ConnectSQL1Res', function(data) {
		logger.info('Received Java_ConnectSQL1Res');
		socket.broadcast.emit('Web_ConnectSQL1Res', data);
	});
	
	socket.on('Web_ConnectSQL2Req', function(data) {
		if(Object.keys(data).length !== 0 && data.constructor === Object) {
			logger.info('Received Web_ConnectSQL2Req');
			socket.broadcast.emit('Java_ConnectSQL2Req', data);
		}
	});
	socket.on('Java_ConnectSQL2Res', function(data) {
		logger.info('Received Java_ConnectSQL2Res');
		socket.broadcast.emit('Web_ConnectSQL2Res', data);
	});
	
	socket.on('Web_Rules1Req', function(data) {
		if(Object.keys(data).length !== 0 && data.constructor === Object) {
			logger.info('Received Web_Rules1Req');
			socket.broadcast.emit('Java_Rules1Req', data);
		}
	});
	socket.on('Java_Rules1Res', function(data) {
		logger.info('Received Java_Rules1Res');
		socket.broadcast.emit('Web_Rules1Res', data);
	});
	
	socket.on('Web_Rules2Req', function(data) {
		if(Object.keys(data).length !== 0 && data.constructor === Object) {
			logger.info('Received Web_Rules2Req');
			socket.broadcast.emit('Java_Rules2Req', data);
		}
	});
	socket.on('Java_Rules2Res', function(data) {
		logger.info('Received Java_Rules2Res');
		socket.broadcast.emit('Web_Rules2Res', data);
	});
});

// every 30 seconds, print all connected clients to the console
setInterval(function() {
	console.log(JSON.stringify(connections));
}, 30000);

// start the server
server.listen(8000, '199.15.250.44');
