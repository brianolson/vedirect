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
  "AC_OUT_S": {"n":"AC Power", "u":"VA","d":5},
  "battery temperature": {"n":"battery temperature", "u":"°C", "m":0.01, "offset": -273.15, "d":2},
  "charger internal temperature": {"n":"charger temperature", "u":"°C", "m":0.01, "d":2}
};
var extractTimeXy = function(d, name, veopt) {
  var ob = {};
  var xy = [];
  var tLimitMin = veopt.tmin;
  var tLimitMax = veopt.tmax;
  for (var i = 0, rec; rec = d[i]; i++) {
    ob = Object.assign(ob, rec);
    var time = ob["_t"];
    if (tLimitMin && (time < tLimitMin)) {
      continue;
    }
    if (tLimitMax && (time > tLimitMax)) {
      continue;
    }
    var val = ob[name];
    if ((val != null) && (val != undefined)) {
      xy.push(time);
      xy.push(val);
    }
  }
  return xy;
};
var maxGapTrim = function(xy, maxgap) {
  var prevt = xy[xy.length - 2];
  for (var i = xy.length - 4; i >= 0; i -= 2) {
    var dt = prevt - xy[i];
    if (dt > maxgap) {
      return xy.slice(i+2);
    }
  }
  return xy;
};
window.bve = window.bve || {};
window.bve.plotResponse = function(ob, elemid, veopt) {
  var plots = document.getElementById(elemid);
  veopt = veopt || {};
  var data = ob.d;
  var html = "";
  var toplot = {};
  var mint = data[0]["_t"];
  var maxt = data[0]["_t"];
  var datavars = {};
  for (var i = 1, rec; rec = data[i]; i++) {
    var t = data[i]["_t"];
    if (t < mint) {
      mint = t;
    }
    if (t > maxt) {
      maxt = t;
    }
    for (var dk in data[i]) {
      datavars[dk] = true;
    }
  }
  // TODO: trim data before some absolute time previous to now, it prevents the problem of data stopping weeks ago and then a few new points added now as it starts again. OR ad some clever explicit discontinuity mode to plotlib |^-_-|"3 week gap|^-_-|
  var minxlabel = (new Date(mint)).toLocaleString();
  var maxxlabel = (new Date(maxt)).toLocaleString();
  for (var varname in plottables) {
    if (!datavars[varname]) {
      continue;
    }
    if (veopt.plotvars) {
      var has = false;
      for (var i = 0, pvi; pvi = veopt.plotvars[i]; i++) {
	if (pvi == varname) {
	  has = true;
	  break;
	}
      }
      if (!has) {
	continue;
      }
    }
    var localmint = mint;
    var localminxlabel = minxlabel;
    var xy = extractTimeXy(data, varname, veopt);
    if (veopt.maxgap) {
      xy = maxGapTrim(xy, veopt.maxgap);
      localmint = xy[0];
      localminxlabel = (new Date(localmint)).toLocaleString();
    }
    if (xy && (xy.length > 0)) {
      var opt = {"xlabels":[localminxlabel, maxxlabel], "minx":localmint, "maxx":maxt};
      var nicename = plottables[varname]["n"] || varname;
      if (plottables[varname]["u"]) {
	nicename += " (" + plottables[varname]["u"] + ")";
      }
      html += "<div class=\"plot\"><div class=\"plotl\">"+nicename+"</div><canvas class=\"plotc\" id=\"plot_" + elemid + "_" + varname + "\"></canvas></div>";
      var multiplier = plottables[varname]["m"];
      if (multiplier) {
	for(var i = 1; i < xy.length; i += 2){
	  xy[i] = xy[i] * multiplier;
	}
      }
      var offset = plottables[varname]["offset"];
      if (offset) {
	for(var i = 1; i < xy.length; i += 2){
	  xy[i] = xy[i] + offset;
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
    lineplot(document.getElementById("plot_"+elemid + "_" + varname), xy, plotopts);
  }
};
})();
