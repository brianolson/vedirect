  var GET = function(url, handler) {
    var http = new XMLHttpRequest();
    http.onreadystatechange = handler;
    http.timeout = 9000;
    http.open("GET",url,true);
    http.send();
  };
var plottables = {
  "V": {"n":"battery voltage","u":"mV"},
  "VPV": {"n":"panel voltage","u":"mV"},
  "PPV": {"n":"panel power", "u":"W"},
  "I": {"n":"current", "u":"mA"},
  "T": {"n":"temperature", "u":"°C"},
  "P": {"n":"power", "u":"W"},
  "AC_OUT_V": {"n":"AC Volts", "u":"0.01V"},
  "AC_OUT_I": {"n":"AC Amps", "u":"0.1A"},
  "AC_OUT_S": {"n":"AC Power", "u":"VA"}
};
var extractTimeXy = function(d, name) {
  var ob = {};
  var xy = [];
  for (var i = 0, rec; rec = d[i]; i++) {
    ob = Object.assign(ob, rec);
    var time = ob["_t"];
    var val = ob[name];
    if ((val != null) && (val != undefined)) {
      xy.push(time);
      xy.push(val);
    }
  }
  return xy;
};
  var dataHandler = function() {
    if (this.readyState == 4 && this.status == 200) {
      var plots = document.getElementById('plots');
      var ob = JSON.parse(this.responseText);
      var data = ob.d;
      var html = "";
      var toplot = {};
      var mint = data[0]["_t"];
      var maxt = data[0]["_t"];
      for (var i = 1, rec; rec = data[i]; i++) {
	var t = data[i]["_t"];
	if (t < mint) {
	  mint = t;
	}
	if (t > maxt) {
	  maxt = t;
	}
      }
      var minxlabel = (new Date(mint)).toLocaleString();
      var maxxlabel = (new Date(maxt)).toLocaleString();
      for (var varname in plottables) {
	var v0 = data[0][varname];
	if ((v0 === undefined) || (v0 == null)) {
	  continue;
	}
	var xy = extractTimeXy(data, varname);
	if (xy && (xy.length > 0)) {
	  var nicename = plottables[varname]["n"] || varname;
	  if (plottables[varname]["u"]) {
	    nicename += " (" + plottables[varname]["u"] + ")";
	  }
	  html += "<div class=\"plot\"><canvas class=\"plotc\" id=\"plot_" + varname + "\"></canvas><div class=\"plotl\">"+nicename+"</div></div>";
	  toplot[varname] = xy;
	}
      }
      plots.innerHTML = html;
      var plotopts = {"xlabels":[minxlabel, maxxlabel], "minx":mint, "maxx":maxt};
      for (var varname in toplot) {
	var xy = toplot[varname];
	lineplot(document.getElementById("plot_"+varname), xy, plotopts);
      }
    }
  };
  GET('/ve.json', dataHandler);
