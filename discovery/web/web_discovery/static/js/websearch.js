function getCookie(name) {
  var r = document.cookie.match("\\b" + name + "=([^;]*)\\b");
  return r ? r[1] : undefined;
}

function encodeGetParams(params) {
  return Object.entries(params)
    .filter(function(kv) { return kv[1] !== undefined; })
    .map(function(kv) { return kv.map(encodeURIComponent).join("="); })
    .join("&");
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

var keywords_input = document.getElementById('keywords');
var results_list = document.getElementById('results');

document.getElementById('search-form').addEventListener('submit', function(e) {
  e.preventDefault();

  var keywords = keywords_input.value;

  postJSON('/pages', {keywords: keywords})
  .then(function(result) {
    results_list.innerHTML = '';
    for(var i = 0; i < result.pages.length; ++i) {
      var page = result.pages[i];

      // Create element for page
      var page_elem = document.createElement('div');
      page_elem.className = 'list-group-item';
      page_elem.innerHTML =
        '<div class="form-row align-items-center">' +
        '  <div class="col-md-1 my-1">' +
        '      <input type="checkbox" class="form-control" checked>' +
        '  </div>' +
        '  <div class="col-md-8 my-1">' +
        '    <span class="mr-sm-2">' + page.title + '</span><br>' +
        '    <small><code>' + page.url + '</code></small>' +
        '  </div>' +
        '  <div class="col-auto my-1">' +
        '    <button type="submit" class="btn btn-outline-primary">Ingest <span aria-hidden="true">&raquo;</span></button>' +
        '  </div>' +
        '  <div class="row col-md-12">' +
        '    <ul class="list-unstyled col-md-10 offset-md-1">' +
        '    </ul>' +
        '  </div>' +
        '</div>';
      var file_list = page_elem.firstElementChild.lastElementChild.firstElementChild;
      console.log(file_list);

      for(var j = 0; j < page.files.length; ++j) {
        var file = page.files[j];

        // Create element for file
        var file_elem = document.createElement('li');
        var file_format_style = 'badge-success';
        if(file.format != 'CSV') {
          file_format_style = 'badge-warning';
        }
        file_elem.innerHTML =
          '<input type="checkbox" checked> ' +
          '<code>' + file.url + '</code> ' +
          '<span class="badge badge-pill ' + file_format_style + '">' + file.format + '</span>';

        file_list.appendChild(file_elem);
      }

      results_list.appendChild(page_elem);
    }
  });
});
