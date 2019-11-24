(function($) { // on page load and jQuery is ready
	var socket = io.connect('https://crustycrab.proxcp.com:8000', { secure:true }); // connect to the socket server
	socket.on('connect', function() {
		console.log('Connected to socket');
		socket.emit('addUserConnection', Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15));
	});
	socket.on('reconnecting', function() {
		console.log('Lost connection to socket! Attempting to reconnect...');
	});

	var Cleanix = {};
	// use HTML5 local storage to store data between web pages
	if(typeof(Storage) !== "undefined") {
		Cleanix.cols1 = null;
		Cleanix.cols2 = null;
		Cleanix.save = function(key, value) {
			localStorage.setItem(key, JSON.stringify(value));
		};
		Cleanix.get = function(key) {
			return JSON.parse(localStorage.getItem(key));
		};
		Cleanix.getCols = function() {
			Cleanix.cols1 = Cleanix.get("cols1");
			Cleanix.cols2 = Cleanix.get("cols2");
			var content = '<form id="form_rules1">';
			var content2 = '<form id="form_rules2">';
			for(var k in Cleanix.cols1) {
				if(Cleanix.cols1.hasOwnProperty(k)) {
					content += '<h5>Table: ' + k + '</h5><table class="table table-hover">';
					content += '<input type="hidden" value="'+k+'" name="table_name" />';
					content += '<tr><th>ID</th><th>Name</th><th style="width:12.5%">Check?</th><th style="width:12.5%">Nullable?</th><th style="width:12.5%">Data Type</th><th>Checking Rule</th><th style="width:12.5%">Smart Filling</th><th>Filling Rule</th></tr>';
					for(var i = 0; i < Cleanix.cols1[k].length; i++) {
						content += '<tr>';
						content += '<td>' + (i+1) + '</td>';
						content += '<td><input type="text" value="'+Cleanix.cols1[k][i][0]+'" class="form-control" readonly name="column_name" /></td>';
						content += '<td><select name="need_to_check" class="form-control"><option value="yes">yes</option><option value="no">no</option></select></td>';
						if(Cleanix.cols1[k][i][2] == "NO")
							content += '<td><select name="can_be_empty" class="form-control"><option value="no">no</option><option value="yes">yes</option></select></td>';
						else
							content += '<td><select name="can_be_empty" class="form-control"><option value="yes">yes</option><option value="no">no</option></select></td>';
						content += '<td><input type="text" value="'+Cleanix.cols1[k][i][1]+'" class="form-control" readonly name="data_type" /></td>';
						if(Cleanix.cols1[k][i][1].indexOf("int") >= 0) {
							content += '<td><input name="cr_min" class="form-control" type="number" placeholder="Min" /><input name="cr_max" class="form-control" type="number" placeholder="Max" /></td>';
						}else if(Cleanix.cols1[k][i][1].indexOf("varchar") >= 0) {
							content += '<td><input name="cr_regex" class="form-control" type="text" placeholder="regex" /></td>';
						}else if(Cleanix.cols1[k][i][1].indexOf("float") >= 0) {
							content += '<td><input name="cr_min" class="form-control" type="number" placeholder="Min" /><input name="cr_max" class="form-control" type="number" placeholder="Max" /></td>';
						}else{
							content += '<td><input name="cr_early" class="form-control" type="datetime-local" /><input name="cr_late" class="form-control" type="datetime-local" /></td>';
						}
						content += '<td><select name="intelligent" class="form-control"><option value="no">no</option><option value="yes">yes</option></select></td>';
						content += '<td><input name="filling_rule" type="text" class="form-control" placeholder="Dumb fill value" /></td>';
						content += '</tr>';
					}
					content += '</table>';
				}
			}
			for(var k in Cleanix.cols2) {
				if(Cleanix.cols2.hasOwnProperty(k)) {
					content2 += '<h5>Table: ' + k + '</h5><table class="table table-hover">';
					content2 += '<input type="hidden" value="'+k+'" name="table_name" />';
					content2 += '<tr><th>ID</th><th>Name</th><th style="width:12.5%">Check?</th><th style="width:12.5%">Nullable?</th><th style="width:12.5%">Data Type</th><th>Checking Rule</th><th style="width:12.5%">Smart Filling</th><th>Filling Rule</th></tr>';
					for(var i = 0; i < Cleanix.cols2[k].length; i++) {
						content2 += '<tr>';
						content2 += '<td>' + (i+1) + '</td>';
						content2 += '<td><input type="text" value="'+Cleanix.cols2[k][i][0]+'" class="form-control" readonly name="column_name" /></td>';
						content2 += '<td><select name="need_to_check" class="form-control"><option value="yes">yes</option><option value="no">no</option></select></td>';
						if(Cleanix.cols2[k][i][2] == "NO")
							content2 += '<td><select name="can_be_empty" class="form-control"><option value="no">no</option><option value="yes">yes</option></select></td>';
						else
							content2 += '<td><select name="can_be_empty" class="form-control"><option value="yes">yes</option><option value="no">no</option></select></td>';
						content2 += '<td><input type="text" value="'+Cleanix.cols2[k][i][1]+'" class="form-control" readonly name="data_type" /></td>';
						if(Cleanix.cols2[k][i][1].indexOf("int") >= 0) {
							content2 += '<td><input name="cr_min" class="form-control" type="number" placeholder="Min" /><input name="cr_max" class="form-control" type="number" placeholder="Max" /></td>';
						}else if(Cleanix.cols2[k][i][1].indexOf("varchar") >= 0) {
							content2 += '<td><input name="cr_regex" class="form-control" type="text" placeholder="regex" /></td>';
						}else if(Cleanix.cols2[k][i][1].indexOf("float") >= 0) {
							content2 += '<td><input name="cr_min" class="form-control" type="number" placeholder="Min" /><input name="cr_max" class="form-control" type="number" placeholder="Max" /></td>';
						}else{
							content2 += '<td><input name="cr_early" class="form-control" type="datetime-local" /><input name="cr_late" class="form-control" type="datetime-local" /></td>';
						}
						content2 += '<td><select name="intelligent" class="form-control"><option value="no">no</option><option value="yes">yes</option></select></td>';
						content2 += '<td><input name="filling_rule" type="text" class="form-control" placeholder="Dumb fill value" /></td>';
						content2 += '</tr>';
					}
					content2 += '</table>';
				}
			}
			content += '<button type="submit" class="btn btn-success" id="submit_rules1">Save #1 Rules</button></form>';
			content2 += '<button type="submit" class="btn btn-success" id="submit_rules2">Save #2 Rules</button></form>';
			$('#db1rules').append(content);
			$('#db2rules').append(content2);
		};
	}else{
		alert("Unsupported browser! Your browser needs to support HTML local storage.");
	}

	// is the string empty?
	function isEmpty(str) {
		return (!str || 0 === str.length);
	}
	// is the string blank?
	function isBlank(str) {
		return (!str || /^\s*$/.test(str));
	}
	String.prototype.isEmpty = function() {
		return (this.length === 0 || !this.trim());
	};
	// is the string alphanumeric?
	function isAlphaNum(str) {
		var exp = /^[0-9a-z]+$/;
		if(str.match(exp)) return true;
		else return false;
	}
	// save the user-defined rules as a JSON object
	function formatRules(arr) {
		var obj = {};
		var rulenum = -1;
		var tn = '';
		for(var i = 0; i < arr.length; i++) {
			if(arr[i]["name"] == "table_name") {
				tn = arr[i]["value"];
				obj[tn] = {};
				rulenum = -1;
			}else if(arr[i]["name"] == "column_name") {
				rulenum += 1;
				obj[tn][''+rulenum] = {};
				obj[tn][''+rulenum]['column_name'] = arr[i]["value"];
			}else if(arr[i]["name"] == "can_be_empty") {
				obj[tn][''+rulenum]['can_be_empty'] = arr[i]["value"];
			}else if(arr[i]["name"] == "cr_min") {
				obj[tn][''+rulenum]['cr_min'] = arr[i]["value"];
			}else if(arr[i]["name"] == "cr_max") {
				obj[tn][''+rulenum]['cr_max'] = arr[i]["value"];
			}else if(arr[i]["name"] == "intelligent") {
				obj[tn][''+rulenum]['intelligent'] = arr[i]["value"];
			}else if(arr[i]["name"] == "filling_rule") {
				obj[tn][''+rulenum]['filling_rule'] = arr[i]["value"];
			}else if(arr[i]["name"] == "cr_regex") {
				obj[tn][''+rulenum]['cr_regex'] = arr[i]["value"];
			}else if(arr[i]["name"] == "cr_early") {
				obj[tn][''+rulenum]['cr_early'] = arr[i]["value"];
			}else if(arr[i]["name"] == "cr_late") {
				obj[tn][''+rulenum]['cr_late'] = arr[i]["value"];
			}else if(arr[i]["name"] == "need_to_check") {
				obj[tn][''+rulenum]['need_to_check'] = arr[i]["value"];
			}else if(arr[i]["name"] == "data_type") {
				obj[tn][''+rulenum]['data_type'] = arr[i]["value"];
			}
		}
		return obj;
	}
	// send db information to the server
	$('#connect_hyracks').click(function(e) {
		e.preventDefault();
		$('#connect_error').html('');
		$(this).prop("disabled", true);
		var ccip = $('#ccip').val();
		var ccp = $('#ccp').val();
		var ncn1 = $('#ncn1').val();
		var ncn2 = $('#ncn2').val();
		if((isEmpty(ccip) || isBlank(ccip) || ccip.isEmpty()) || (isEmpty(ccp) || isBlank(ccp) || ccp.isEmpty() || !isAlphaNum(ccp)) || (isEmpty(ncn1) || isBlank(ncn1) || ncn1.isEmpty()) || (isEmpty(ncn2) || isBlank(ncn2) || ncn2.isEmpty())) {
			$('#connect_error').html('Error: invalid form 1 values.');
			$(this).prop("disabled", false);
		}else{
			var data = {
				ccip: ccip,
				ccp: ccp,
				ncn1: ncn1,
				ncn2: ncn2
			};
			$('#connect_error').html('');
			socket.emit('Web_ConnectHyracksReq', data);
		}
	});
	socket.on('Web_ConnectHyracksRes', function(res) {
		console.log(res);
	});
	
	// send db information to the server
	$('#sql1').click(function(e) {
		e.preventDefault();
		$('#connect_error').html('');
		$(this).prop("disabled", true);
		var sql1ip = $('#sql1ip').val();
		var sql1user = $('#sql1user').val();
		var sql1pw = $('#sql1pw').val();
		var sql1db = $('#sql1db').val();
		if((isEmpty(sql1ip) || isBlank(sql1ip) || sql1ip.isEmpty()) || (isEmpty(sql1user) || isBlank(sql1user) || sql1user.isEmpty()) || (isEmpty(sql1db) || isBlank(sql1db) || sql1db.isEmpty())) {
			$('#connect_error').html('Error: invalid form 2 values.');
			$(this).prop("disabled", false);
		}else{
			var data = {
				sql1ip: sql1ip,
				sql1user: sql1user,
				sql1pw: sql1pw,
				sql1db: sql1db
			};
			$('#connect_error').html('');
			socket.emit('Web_ConnectSQL1Req', data);
		}
	});
	socket.on('Web_ConnectSQL1Res', function(res) {
		Cleanix.cols1 = res;
		Cleanix.save("cols1", Cleanix.cols1);
		console.log("cols1 set");
	});
	
	// send db information to the server
	$('#sql2').click(function(e) {
		e.preventDefault();
		$('#connect_error').html('');
		$(this).prop("disabled", true);
		var sql2ip = $('#sql2ip').val();
		var sql2user = $('#sql2user').val();
		var sql2pw = $('#sql2pw').val();
		var sql2db = $('#sql2db').val();
		if((isEmpty(sql2ip) || isBlank(sql2ip) || sql2ip.isEmpty()) || (isEmpty(sql2user) || isBlank(sql2user) || sql2user.isEmpty()) || (isEmpty(sql2db) || isBlank(sql2db) || sql2db.isEmpty())) {
			$('#connect_error').html('Error: invalid form 3 values.');
			$(this).prop("disabled", false);
		}else{
			var data = {
				sql2ip: sql2ip,
				sql2user: sql2user,
				sql2pw: sql2pw,
				sql2db: sql2db
			};
			$('#connect_error').html('');
			socket.emit('Web_ConnectSQL2Req', data);
		}
	});
	socket.on('Web_ConnectSQL2Res', function(res) {
		Cleanix.cols2 = res;
		Cleanix.save("cols2", Cleanix.cols2);
		console.log("cols2 set");
	});
	
	// send db1 rule set to the server
	$('#db1rules').on('click', '#submit_rules1', function(e) {
		e.preventDefault();
		$(this).prop("disabled", true);
		var data = formatRules($('#form_rules1').serializeArray());
		socket.emit('Web_Rules1Req', data);
	});
	socket.on('Web_Rules1Res', function(res) {
		console.log("rules 1 set");
	});
	
	// send db2 rule set to the server
	$('#db2rules').on('click', '#submit_rules2', function(e) {
		e.preventDefault();
		$(this).prop("disabled", true);
		var data = formatRules($('#form_rules2').serializeArray());
		socket.emit('Web_Rules2Req', data);
	});
	socket.on('Web_Rules2Res', function(res) {
		console.log("rules 2 set");
	});
	
	// generate rule definition tables on page load for settings.html
	if(document.URL.indexOf("settings.html") >= 0) {
		Cleanix.getCols();
	}
})(jQuery);