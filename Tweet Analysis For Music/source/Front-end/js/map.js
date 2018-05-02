google.charts.load('upcoming', { 'packages': ['map'] });
google.charts.setOnLoadCallback(drawMap);

function drawMap() {
    //API call
    var data_file = "https://api.mlab.com/api/1/databases/sample/collections/query4?apiKey=00tOwA90xqI3pIE8rnf2vGdeMvABLrWx";
    var http_request = new XMLHttpRequest();

    http_request.onreadystatechange = function() {
        if (http_request.readyState == 4) {
            var jsonObj = JSON.parse(http_request.responseText);
            var data = new google.visualization.DataTable();
            data.addColumn('string', 'Location');
            data.addColumn('string', 'Number of users');
            for (var i = 0; i < jsonObj.length; i++) {
                data.addRows([
                    [jsonObj[i].location, "City: "+jsonObj[i].location+" | No of Tweets: "+jsonObj[i].users]
                ]);
            }
            var options = {
                zoomLevel: 3,
                showTooltip: true,
                showInfoWindow: true,
                icons:{
                    default:{
                        normal:'http://icons.iconarchive.com/icons/graphics-vibe/media-pin-social/48/twitter-icon.png'
                    }
                }
            };

            var map = new google.visualization.Map(document.getElementById('map_chart'));

            map.draw(data, options);
        }
    }
    http_request.open("GET", data_file, true);
    http_request.send();
};