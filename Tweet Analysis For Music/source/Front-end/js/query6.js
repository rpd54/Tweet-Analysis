google.charts.load('current', {'packages':['corechart']});
google.charts.setOnLoadCallback(drawChart);

function drawChart() {
    //API call
    var data_file = "https://api.mlab.com/api/1/databases/sample/collections/names?apiKey=00tOwA90xqI3pIE8rnf2vGdeMvABLrWx";
    var http_request = new XMLHttpRequest();

    http_request.onreadystatechange = function(){

        if (http_request.readyState == 4  ){
            var jsonObj = JSON.parse(http_request.responseText);
            var data = new google.visualization.DataTable();
            data.addColumn('string', 'Names');
            data.addColumn('number', 'tweet volume');
            for(var i=0;i<jsonObj.length;i++)
            {
                data.addRows([
                    [jsonObj[i].names, jsonObj[i].tweet_volume]
                ]);
            }
            // Set chart options
            var options = {
                'title':"Histogram",
                'width':1000, 'height':450, legend: { position: 'none' }};

            var chart = new google.visualization.Histogram(document.getElementById('pie_chart1'));
            chart.draw(data, options);
        }
    }

    http_request.open("GET", data_file, true);
    http_request.send();
}