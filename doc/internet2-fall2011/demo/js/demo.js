/*
 * Justin Samuel <jsamuel@cs.arizona.edu>
 * Date: 2008-03-19
 *
 * Periodically checks a url for lat/long data to map using the Google
 * Maps API.
 *
 * JSON data is expected as:
 *     [[float lat, float long, string name, int version], ...]
 */

// Frequency in milliseconds that the client should check for new data.
var UPDATE_FREQUENCY = 5000;

// Where the json data is grabbed from.
var UPDATE_URL = "/gec4demo/data";

// The id of the page element that has the google map.
var MAP_ELEMENT_ID = "map_canvas";

// Setup different color markers for different client reported versions.
var iconSize = new GSize(32,32);
var blueIcon = new GIcon(G_DEFAULT_ICON);
blueIcon.image = "http://www.google.com/intl/en_us/mapfiles/ms/micons/blue-dot.png";
blueIcon.iconSize = iconSize;
var redIcon = new GIcon(G_DEFAULT_ICON);
redIcon.image = "http://www.google.com/intl/en_us/mapfiles/ms/micons/red-dot.png";
redIcon.iconSize = iconSize;
var greenIcon = new GIcon(G_DEFAULT_ICON);
greenIcon.image = "http://www.google.com/intl/en_us/mapfiles/ms/micons/green-dot.png";
greenIcon.iconSize = iconSize;
var yellowIcon = new GIcon(G_DEFAULT_ICON);
yellowIcon.image = "http://www.google.com/intl/en_us/mapfiles/ms/micons/yellow-dot.png";
yellowIcon.iconSize = iconSize;
var markerOptions = [ {icon:blueIcon}, {icon:redIcon}, {icon:greenIcon}, {icon:yellowIcon} ];

var map;
var data = {};
var intervalId;

function initialize() {
    if (!GBrowserIsCompatible()) {
        showMessage("Your browser will not work with this demo.");
        return;
    }
    clearMessage();
    intervalId = setInterval(getData, UPDATE_FREQUENCY);
    map = new GMap2(document.getElementById(MAP_ELEMENT_ID));
    map.setCenter(new GLatLng(20, 0), 3);
    var mapControl = new GMapTypeControl();
    map.addControl(mapControl);
    map.addControl(new GLargeMapControl());
}

function showMessage(msg) {
    msgDiv = document.getElementById("message");
    msgDiv.innerHTML = msg;
    msgDiv.style.display = "block";
}

function clearMessage() {
    document.getElementById("message").style.display = "none";
}

function getData() {
    var req = new XMLHttpRequest();
    req.open("GET", UPDATE_URL, true);
    req.onreadystatechange = function (e) {
        if (req.readyState == 4) {
            if(req.status == 200) {
                updateData(eval(req.responseText));
                clearMessage();
            } else {
                showMessage("Error retrieving data.");
		// This could be used to halt future updates.
                //clearInterval(intervalId);
            }
        }
    };
    req.send(null);
}

/*
 * Look through the newData to see if we should add or update points.
 */
function updateData(newData) {
    for (var i = 0; i < newData.length; i++) {
        var latNum = newData[i][0];
        var lngNum = newData[i][1];
        var site = newData[i][2];
        var version = newData[i][3];
        var lat = String(latNum);
        var lng = String(lngNum);
        if (!data[lat]) {
            data[lat] = {}
        }
        if (!data[lat][lng]) {
            data[lat][lng] = {}
        }
        // If this point already exists, see if we should update the marker
        // color because the version has changed.
        if (data[lat][lng][site]) {
            if (data[lat][lng][site].version == version) {
                continue;
            } 
            map.removeOverlay(data[lat][lng][site].marker);
        }
        // Add the point to the map (either it doesn't exist or we just
        // removed it because the version has changed).
        // Randomize the point a bit so that multiple of the same lat/long
        // don't overlap.
        var point = new GLatLng(latNum + Math.random() / 4, lngNum + Math.random() / 4);
        var options = markerOptions[version - 1];
        // This seems to work without a need to clone the options object.
        options.title = site;
        var marker = new GMarker(point, options);
        //var marker = new GMarker(point);
        data[lat][lng][site] = {site:site, version:version, marker:marker};
        map.addOverlay(marker);
    }
}

