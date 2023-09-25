(function(){
var plottables = {
  "V": {"n":"battery voltage","u":"V","m":0.001,"d":5},
  "VPV": {"n":"panel voltage","u":"V","m":0.001,"d":5},
  "PPV": {"n":"panel power", "u":"W","d":5},
  "I": {"n":"current", "u":"A","m":0.001,"d":5},
  "T": {"n":"temperature", "u":"°C","d":3},
  "P": {"n":"power", "u":"W","d":5},
  "AC_OUT_V": {"n":"AC Volts", "u":"V","m":0.01,"d":5},
  "AC_OUT_I": {"n":"AC Amps", "u":"A","m":0.1,"d":5},
  "AC_OUT_S": {"n":"AC Power", "u":"VA","d":5}
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
window.bve = window.bve || {};
window.bve.plotResponse = function(ob, elemid) {
  var plots = document.getElementById(elemid);
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
      var opt = {"xlabels":[minxlabel, maxxlabel], "minx":mint, "maxx":maxt};
      var nicename = plottables[varname]["n"] || varname;
      if (plottables[varname]["u"]) {
	nicename += " (" + plottables[varname]["u"] + ")";
      }
      html += "<div class=\"plot\"><canvas class=\"plotc\" id=\"plot_" + varname + "\"></canvas><div class=\"plotl\">"+nicename+"</div></div>";
      var multiplier = plottables[varname]["m"];
      if (multiplier) {
	for(var i = 1; i < xy.length; i += 2){
	  xy[i] = xy[i] * multiplier;
	}
      }
      var ydecimals = plottables[varname]["d"];
      if (ydecimals) {
	var miny = xy[1];
	var maxy = xy[1];
	var lasty = xy[1];
	for (var i = 3; i < xy.length; i += 2) {
	  var y = xy[i];
	  if (y<miny) {miny = y;}
	  if (y>maxy) {maxy = y;}
	  lasty = y;
	}
	opt["ylabels"] =[miny.toPrecision(ydecimals), maxy.toPrecision(ydecimals), lasty.toPrecision(ydecimals)];
      }
      toplot[varname] = {"xy":xy, "opt":opt};
    }
  }
  plots.innerHTML = html;
  for (var varname in toplot) {
    var xy = toplot[varname]["xy"];
    var plotopts = toplot[varname]["opt"];
    lineplot(document.getElementById("plot_"+varname), xy, plotopts);
  }
};
})();
