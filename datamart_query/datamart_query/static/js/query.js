function getCookie(name) {
  var r = document.cookie.match("\\b" + name + "=([^;]*)\\b");
  return r ? r[1] : undefined;
}

function postJSON(url='', data={}, args) {
  if(args) {
    args = '&' + encodeGetParams(args);
  } else {
    args = '';
  }
  return fetch(
    url + '?_xsrf=' + encodeURIComponent(getCookie('_xsrf')) + args,
    {
      credentials: 'same-origin',
      mode: 'same-origin',
      cache: 'no-cache',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json; charset=utf-8'
      },
      body: JSON.stringify(data)
    }
  ).then(function(response) {
    if(response.status != 200) {
      throw "Status " + response.status;
    }
    return response.json();
  });
}

function formatSize(bytes) {
    var i = 0;
    var units = [' B', ' kB', ' MB', ' GB', ' TB', 'PB', 'EB', 'ZB', 'YB'];
    while(bytes > 1000) {
        bytes = bytes / 1000;
        i++;
    }

    return bytes.toFixed(1) + units[i];
};

document.getElementById('search-form').addEventListener('submit', function(e) {
  e.preventDefault();
  query = document.getElementById('search-bar').value;
  query = query.split(/[ ,+]+/);
  console.log("Searching:", query);
  postJSON('/query', {keywords: query})
  .then(function(result) {
    var results_div = document.getElementById('results');
    results_div.innerHTML = '';
    for(var i = 0; i < result.results.length; ++i) {
      var elem = document.createElement('div');
      var data = result.results[i];
      elem.className = 'col-md-4';
      description = data.meta.description
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&apos;')
        .replace(/\n/g, '<br/>');
      elem.innerHTML = (
        '<div class="card mb-4 shadow-sm">' +
        '  <div class="card-body">' +
        '    <p class="card-text">' + data.id + '</p>' +
        '    <p class="card-text">' + description + '</p>' +
        '    <div class="d-flex justify-content-between align-items-center">' +
        '      <div class="btn-group">' +
        '        <button type="button" class="btn btn-sm btn-outline-secondary">View</button>' +
        '        <button type="button" class="btn btn-sm btn-outline-secondary">Download</button>' +
        '      </div>' +
        '      <small class="text-muted">' + (data.size?formatSize(data.size):'unknown size') + '</small>' +
        '    </div>' +
        '  </div>' +
        '</div>'
      );
      results_div.appendChild(elem);
    }
  }, function(error) {
    console.error("Query failed:", error);
  });
});
