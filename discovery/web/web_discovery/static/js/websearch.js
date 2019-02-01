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

function formatSize(bytes) {
  var i = 0;
  var units = [' B', ' kB', ' MB', ' GB', ' TB', ' PB', ' EB', ' ZB', ' YB'];
  while(bytes > 1000 && i + 1 < units.length) {
    bytes = bytes / 1000;
    i++;
  }

  return bytes.toFixed(1) + units[i];
}

var keywords_input = document.getElementById('keywords');
var results_list = document.getElementById('results');

document.getElementById('search-form').addEventListener('submit', function(e) {
  e.preventDefault();

  var keywords = keywords_input.value;

  document.getElementById('processing').style.display = 'block';
  postJSON('/pages', {keywords: keywords})
  .then(function(result) {
    console.log("Got results:", result);
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
      var file_list = page_elem.querySelector('ul');

      // Wire the checkbox
      var checkbox = page_elem.querySelector('input[type=checkbox]');
      (function(checkbox, file_list) {
        checkbox.addEventListener('change', function(e) {
          var checkboxes = file_list.querySelectorAll('input[type=checkbox]');
          for(var j = 0; j < checkboxes.length; ++j) {
            if(!checkboxes[j].disabled) {
              checkboxes[j].checked = this.checked;
            }
          }
        });
      })(checkbox, file_list);

      // Fill in the list of files
      buildFileList(file_list, page.files);

      // Wire the "ingest" button
      setupIngestButton(page_elem.querySelector('button'), file_list);

      results_list.appendChild(page_elem);
    }
  },
  function(error) {
    alert("Error getting search results", error);
  })
  .catch(function(error) {
    alert("Error processing search results", error);
  })
  .then(function() {
    document.getElementById('processing').style.display = 'none';
  });
});

function buildFileList(file_list, files) {
  for(var j = 0; j < files.length; ++j) {
    var file = files[j];

    // Create element for file
    var file_elem = document.createElement('li');
    var file_format_style = 'badge-success';
    if(file.format != 'CSV') {
      file_format_style = 'badge-warning';
    }
    var file_size = '???';
    if(file.size) {
      file_size = formatSize(file.size);
    }
    if(!file.status) {
      var status;
      if(j < 20) {
        status = 'checked';
      } else {
        status = '';
      }
      file_elem.innerHTML =
        '<input type="checkbox" ' + status + '> ' +
        '<code>' + file.url + '</code> ' +
        '<a href="' + file.url + '" class="badge badge-pill ' + file_format_style + '">' + file.format + '</a> ' +
        '<span class="badge badge-pill badge-info">' + file_size + '</span>';
    } else if(file.status == 'ingested') {
      file_elem.innerHTML =
        '<a href="' + COORDINATOR_HOST + '/dataset/' + file.dataset_id + '"><code>' + file.url + '</code></a> ' +
        '<a href="' + file.url + '" class="badge badge-pill ' + file_format_style + '">' + file.format + '</a> ' +
        '<span class="badge badge-pill badge-info">' + file_size + '</span>';
    }

    file_list.appendChild(file_elem);
  }
}

function setupIngestButton(button, file_list) {
  button.addEventListener('click', function(e) {
    e.preventDefault();

    // Find files to ingest
    var files = [];
    var file_elems = [];
    for(var i = 0; i < file_list.childElementCount; ++i) {
      var elem = file_list.children[i];
      var checkbox = elem.querySelector('input');
      if(checkbox !== null && !checkbox.disabled && checkbox.checked) {
        files.push(elem.querySelector('code').innerText);
        file_elems.push(elem);
        checkbox.disabled = true;
      }
    }

    console.log("Ingesting files:", files);

    postJSON('/ingest', {files: files})
    .then(function(result) {
      console.log("Got results:", result);

      // Remove the elements we had sent
      for(var i = 0; i < file_elems.length; ++i) {
        file_list.removeChild(file_elems[i]);
      }

      // Add what we were just sent
      buildFileList(file_list, result.files);
    },
    function(error) {
      alert("Error ingesting datasets", error);
      for(var i = 0; i < file_elems.length; ++i) {
        file_elems.disabled = false;
      }
    })
    .catch(function(error) {
      alert("Error processing ingestion response", error);
    });
  });
}
