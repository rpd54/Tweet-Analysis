
google.charts.load('current', {'packages':['corechart']});
google.charts.setOnLoadCallback(drawChart);

function drawChart() {
    //API call
    var data_file = "https://api.mlab.com/api/1/databases/sample/collections/query2?apiKey=00tOwA90xqI3pIE8rnf2vGdeMvABLrWx";
    var http_request = new XMLHttpRequest();

    http_request.onreadystatechange = function(){

        if (http_request.readyState == 4  ){
            var jsonObj = JSON.parse(http_request.responseText);
            var data = new google.visualization.DataTable();
            data.addColumn('string', 'name');
            data.addColumn('number', 'Count of ReTweets');
            for(var i=0;i<jsonObj.length;i++)
            {
                data.addRows([
                    [jsonObj[i].name, jsonObj[i].retweetsCount]
                ]);
            }
            // Set chart options
            var options = {
                'title':"Line Chart",
                'width':1000,
                'height':450};
             vAxis: {
                     scaleType: 'log'
               }

            var chart = new google.visualization.LineChart(document.getElementById('line_chart'));
            chart.draw(data, options);
        }
    }

    http_request.open("GET", data_file, true);
    http_request.send();
}