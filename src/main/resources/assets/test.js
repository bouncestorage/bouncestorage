function listBuckets() {
    var request = new XMLHttpRequest();
    request.onreadystatechange = function() {
        console.log("List buckets response received");
        if (request.readyState != 4 || request.status != 200) {
            return;
        }
        var selectHTML = "<select id=\"buckets-select\">";
        var json = JSON.parse(request.responseText);
        var containerNames = json["containerNames"];
        var i;
        for (i = 0; i < containerNames.length; ++i) {
            var name = containerNames[i];
            selectHTML += "<option value=\"" + name + "\">" +
                    name + "</option>";
        }
        selectHTML += "</select>"
        document.getElementById("buckets").innerHTML = selectHTML;
    }
    request.open("GET", "/service", true);
    request.send();
}

function listBlobs() {
    var request = new XMLHttpRequest();
    request.onreadystatechange = function() {
        console.log("List blobs response received");
        if (request.readyState != 4 || request.status != 200) {
            return;
        }
        var json = JSON.parse(request.responseText);
        document.getElementById("blob-list").innerHTML = json["blobNames"];
        document.getElementById("bounce-links").innerHTML =
                json["bounceLinkCount"];
    }
    var bucketsSelect = document.getElementById("buckets-select");
    var name = bucketsSelect.options[bucketsSelect.selectedIndex].value;
    request.open("GET", "/container?name=" + name, true);
    request.send();
}

function bounceBlobs() {
    var request = new XMLHttpRequest();
    request.onreadystatechange = function() {
        console.log("Bounce blobs response received");
    }
    var bucketsSelect = document.getElementById("buckets-select");
    var name = bucketsSelect.options[bucketsSelect.selectedIndex].value;
    request.open("POST", "/bounce?name=" + name, true);
    request.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
    request.send();
}
