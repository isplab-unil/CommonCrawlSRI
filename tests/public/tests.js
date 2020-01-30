function loadScript(id, integrity, description, shouldLoad) {
    // Prepare a div that displays the result
    var text = document.createTextNode(description);
    var html = document.createElement('div');
    html.id = 'el-' + id;
    html.className = "pending";
    html.appendChild(text);
    document.getElementById('tests').appendChild(html);

    // Insert the javascript library
    var js, lib = document.getElementsByTagName('script')[0];
    js = document.createElement('script'); 
    js.src = "/tests/" + id + ".js";
    js.integrity = integrity;
    js.onload = function() {
        try {
            if (shouldLoad && eval(id)) {
                html.className = "success";
            } else {
                html.className = "failure";
            }
        } catch (err) {
            html.className = "failure";
        }
    };
    js.onerror = function(err) {
        if (shouldLoad) {
            html.className = "failure";
        } else {
            html.className = "success";
        }
    }
    try {
        lib.parentNode.insertBefore(js, lib);
    } catch (err) {
        html.className = "failure";
    }
};

// curl http://localhost:3000/tests/empty.js | openssl dgst -sha256 -binary | openssl base64 -A
loadScript('empty', '', 'Include script with empty checksum', true)

// curl http://localhost:3000/tests/sha256.js | openssl dgst -sha256 -binary | openssl base64 -A
loadScript('sha256', 'sha256-ivzZrYOz+Cx6RIw7Y+FAB6s4cggUmw9OiBH8pgv/Zkw=', 'Include script with sha256 checksum', true)

// curl http://localhost:3000/tests/sha384.js | openssl dgst -sha384 -binary | openssl base64 -A
loadScript('sha384', 'sha384-eYWSGBnpFRg9ClrjAi4mTSCxFlYYIekngSWn8VhgU/wPRtxMbg/LkwYR9Dph6npg', 'Include script with sha384 checksum', true)

// curl http://localhost:3000/tests/sha512.js | openssl dgst -sha512 -binary | openssl base64 -A
loadScript('sha512', 'sha512-q39H66f+FMudB2deGowsHoMnQcAB8kmNThGg5OuRkceMQjLoubnI7gPbY39r0fjnC88XP/X83mu1lkJH2d6I5Q==', 'Include script with sha256 checksum', true)

// curl http://localhost:3000/tests/malformed.js | openssl dgst -sha256 -binary | openssl base64 -A
loadScript('malformed', 'malformed', 'Include script with malformed checksum', false)

// curl http://localhost:3000/tests/sha256withoutequal.js | openssl dgst -sha256 -binary | openssl base64 -A
loadScript('sha256withoutequal', 'qGNt0AGDBRyjJEonnhaNRd+yV5c8faixokXzRZLm9kU', 'Include script with sha256 checksum', true)

// curl http://localhost:3000/tests/sha256c.js | openssl dgst -sha256 -binary | openssl base64 -A
//loadScript('sha256wrong', 'sha256-OuvYcx8Ajjinq5Q/ZJqhn8m4Q5hMYRhzq7HtznJ/Tos=', 'Exclude script with wrong checksum', false)

// curl http://localhost:3000/tests/sha256d.js | openssl dgst -sha256 -binary | openssl base64 -A
//loadScript('sha256conflict', 'sha256-6jsOvNjQLP4xxhof6GO9zMIpSdBiSr6f/jM24VOWEEA=', 'Include script with conflicting checksum', true)




