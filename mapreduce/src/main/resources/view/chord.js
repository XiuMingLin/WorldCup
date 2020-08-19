load_csv();

function load_csv() {
    $.ajax({
        url: "http://123.56.235.95/MatchWinrate.CSV",
        type: "GET",
        dataType: "TEXT",
        success: function (data) {
            var d = d3.csvParse(data);
            dataPrase(d);
        },
        error: function (XMLHttpRequest, textStatus, errorThrown) {
            alert(XMLHttpRequest.responseText);
        }
    })
}

function dataPrase(d) {
    console.log(d);
    const name = new Array();
    d.forEach(element => {
        if ($.inArray(element.con1, name) == -1) {
            name.push(element.con1);
        }
    });
    console.log(name);

    const matrix = new Array();
    for (let i = 0; i < name.length; i++) {
        matrix.push([]);
        for (let j = 0; j < name.length; j++) {
            matrix[i].push(0);
        }
    }

    d.forEach(element => {
        let i = $.inArray(element.con1, name);
        let j = $.inArray(element.con2, name);
        matrix[i][j] = Number(element.win) + Number(element.draw) + Number(element.def);
    })
    console.log(matrix);

    chord(name, matrix);

    const rate = new Array();
    for (let i = 0; i < name.length; i++) {
        rate.push([]);
        for (let j = 0; j < 4; j++) {
            rate[i].push(0);
        }
    }
    d.forEach(element => {
        let i = $.inArray(element.con1, name);
        rate[i][1] = rate[i][1] + Number(element.win);
        rate[i][2] = rate[i][2] + Number(element.draw);
        rate[i][3] = rate[i][3] + Number(element.def);
    })
    rate.forEach(element => {
        element[0] = element[1] + element[2] + element[3];
    })
    console.log(rate);
    histogram(name, rate);
}


function chord(name, matrix) {

    var width = 600;
    var height = 600;
    var outerRadius = Math.min(width, height) * 0.5 - 40,
        innerRadius = outerRadius - 30;

    var formatValue = d3.formatPrefix(",.0", 1e3);

    var svg = d3
        .select("body")
        .append("svg")
        .attr("width", width)
        .attr("height", height)
        .style("border", "1px dashed #ccc");

    var formatValue = d3.formatPrefix(",.0", 1e3);

    var chord = d3
        .chord()
        .padAngle(0.03)
        .sortSubgroups(d3.descending);

    var arc = d3
        .arc()
        .innerRadius(innerRadius)
        .outerRadius(outerRadius);

    var ribbon = d3.ribbon().radius(innerRadius);

    var color = d3
        .scaleOrdinal()
        .domain(d3.range(4))
        .range(["#000000", "#FFDD89", "#957244", "#F26223"]);

    var g = svg
        .append("g")
        .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")")
        .datum(chord(matrix));

    var group = g
        .append("g")
        .attr("class", "groups")
        .selectAll("g")
        .data(function (chords) {
            return chords.groups;
        })
        .enter()
        .append("g");

    group
        .append("path")
        .style("fill", function (d) {
            return d3.schemeCategory10[d.index];
        })
        .style("stroke", function (d) {
            return d3.rgb(color(d.index)).darker();
        })
        .attr("class", "outerPath")
        .attr("d", arc);
    g
        .selectAll(".outerText")
        .data(chord(matrix).groups)
        .enter()
        .append("text")
        .each(function (d, i) {
            d.angle = (d.startAngle + d.endAngle) / 2;
            d.name = name[i];
        })
        .attr("class", "outText")
        .attr("dy", ".35em")
        .attr("transform", function (d) {
            var result = "rotate(" + d.angle * 180 / Math.PI + ")";
            result += "translate(0," + -1.0 * (outerRadius + 10) + ")";
            if (d.angle > Math.PI * 3 / 4 && d.angle < Math.PI * 5 / 4) {
                result += "rotate(180)";
            }
            return result;
        })
        .text(function (d) {
            return d.name;
        });

    var acinner = d3.ribbon().radius(innerRadius);
    g
        .selectAll(".innerPath")
        .data(chord(matrix))
        .enter()
        .append("path")
        .attr("class", "innerPath")
        .attr("d", acinner)
        .style("fill", function (d) {
            return d3.schemeCategory10[d.source.index];
        });
}

function histogram(name, dataset) {
    const scale = d3.scaleLinear()
        .domain([0, ((d) => {
            let max = 0;
            d.forEach(element => {
                if (element[0] > max)
                    max = element[0];
            })
            return max;
        })(dataset)])
        .range([0, 1500]);

    const svg = d3
        .select("body")
        .append("svg")
        .style("border", "1px dashed #ccc")
        .style("width", "-webkit-fill-available")
        .style("height", "-webkit-fill-available");

    const rectHeight = 60;

    const g1 = svg
        .append("g")
        .attr("transform", "translate(30,30)");

    g1
        .selectAll("rect")
        .data(dataset)
        .enter()
        .append("rect")
        .attr("x", (d, i) => {
            return 20;
        })
        .attr("y", (d, i) => {
            return i * rectHeight;
        })
        .attr("width", (d) => {
            return scale(d[1]);
        })
        .attr("height", rectHeight - 20)
        .attr("fill", "blue")

    const g2 = svg
        .append("g")
        .attr("transform", "translate(30,30)");

    g2
        .selectAll("rect")
        .data(dataset)
        .enter()
        .append("rect")
        .attr("x", (d, i) => {
            return 20 + scale(d[1]);
        })
        .attr("y", (d, i) => {
            return i * rectHeight;
        })
        .attr("width", (d) => {
            return scale(d[2]);
        })
        .attr("height", rectHeight - 20)
        .attr("fill", "green");

    const g3 = svg
        .append("g")
        .attr("transform", "translate(30,30)");

    g3
        .selectAll("rect")
        .data(dataset)
        .enter()
        .append("rect")
        .attr("x", (d, i) => {
            return 20 + scale(d[1]) + scale(d[2]);
        })
        .attr("y", (d, i) => {
            return i * rectHeight;
        })
        .attr("width", (d) => {
            return scale(d[3]);
        })
        .attr("height", rectHeight - 20)
        .attr("fill", "red");

    const g4 = svg
        .append("g")
        .attr("transform", "translate(30,30)");

    g4
        .selectAll("text")
        .data(dataset)
        .enter()
        .append("text")
        .attr("x", (d, i) => {
            return 25 + scale(d[1]) + scale(d[2]) + scale(d[3]);
        })
        .attr("y", (d, i) => {
            return i * rectHeight + 15;
        })
        .text((d, i) => {
            return name[i];
        });

    const g_text1 = svg
        .append("g")
        .attr("transform", "translate(30,30)");

    g_text1
        .selectAll("text")
        .data(dataset)
        .enter()
        .append("text")
        .attr("x", (d, i) => {
            return 20;
        })
        .attr("y", (d, i) => {
            return i * rectHeight;
        })
        .text((d, i) => {
            let n = d[1];
            if(n==0)
                return "";
            else
                return n;
        });

    const g_text2 = svg
        .append("g")
        .attr("transform", "translate(30,30)");

    g_text2
        .selectAll("text")
        .data(dataset)
        .enter()
        .append("text")
        .attr("x", (d, i) => {
            return 20 + scale(d[1]);
        })
        .attr("y", (d, i) => {
            return i * rectHeight;
        })
        .text((d, i) => {
            let n = d[2];
            if(n==0)
                return "";
            else
                return n;
        });

    const g_text3 = svg
        .append("g")
        .attr("transform", "translate(30,30)");

    g_text3
        .selectAll("text")
        .data(dataset)
        .enter()
        .append("text")
        .attr("x", (d, i) => {
            return 20 + scale(d[1]) + scale(d[2]);
        })
        .attr("y", (d, i) => {
            return i * rectHeight;
        })
        .text((d, i) => {
            let n = d[3];
            if(n==0)
                return "";
            else
                return n;
        });
}