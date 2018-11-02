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
      mode: 'cors',
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
    while(bytes > 1000 && i + 1 < units.length) {
        bytes = bytes / 1000;
        i++;
    }

    return bytes.toFixed(1) + units[i];
}

document.getElementById('search-form').addEventListener('submit', function(e) {
  e.preventDefault();

  var search = {};

  // Keywords
  var keywords = document.getElementById('keywords').value;
  if(keywords) {
    search.keywords = keywords;
  }

  // Types
  var str_types = [];
  var sem_types = [];
  if(document.getElementById('type-integer').checked) {
    str_types.push('http://schema.org/Integer');
  }
  if(document.getElementById('type-float').checked) {
    str_types.push('http://schema.org/Float');
  }
  if(document.getElementById('type-bool').checked) {
    sem_types.push('http://schema.org/Boolean');
  }
  if(document.getElementById('type-text').checked) {
    str_types.push('http://schema.org/Text');
  }
  if(document.getElementById('type-datetime').checked) {
    sem_types.push('http://schema.org/DateTime');
  }
  if(document.getElementById('type-phone').checked) {
    sem_types.push('https://metadata.datadrivendiscovery.org/types/PhoneNumber');
  }
  if(str_types.length > 0) {
    search.structural_types = str_types;
  }
  if(sem_types.length > 0) {
    search.semantic_types = sem_types;
  }

  console.log("Searching:", search);
  postJSON(QUERY_HOST + '/query', search)
  .then(function(result) {
    console.log("Got " + result.results.length + " results");
    var results_div = document.getElementById('results');
    results_div.innerHTML = '';
    document.getElementById('search-error').style.display = 'none';
    for(var i = 0; i < result.results.length; ++i) {
      var elem = document.createElement('div');
      var data = result.results[i];
      elem.className = 'col-md-4';
      description = data.metadata.description;
      if(description) {
        description = description
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&apos;')
          .replace(/\n/g, '<br/>');
      } else {
        description = "(no description)";
      }
      elem.innerHTML = (
        '<div class="card mb-4 shadow-sm">' +
        '  <div class="card-body">' +
        '    <p class="card-text">' + data.id + '</p>' +
        '    <p class="card-text">' + description + '</p>' +
        '    <div class="d-flex justify-content-between align-items-center">' +
        '      <div class="btn-group">' +
        '        <a href="/dataset/' + data.id + '" class="btn btn-sm btn-outline-secondary">View</a>' +
        '        <a href="' + QUERY_HOST + '/download/' + data.id + '" class="btn btn-sm btn-outline-secondary">Download</a>' +
        '      </div>' +
        '      <small class="text-muted">' + (data.metadata.size?formatSize(data.metadata.size):'unknown size') + '</small>' +
        '    </div>' +
        '  </div>' +
        '</div>'
      );
      results_div.appendChild(elem);
    }
    if(result.results.length == 0) {
      document.getElementById('search-error').style.display = '';
      document.getElementById('search-error').innerText = "No results";
    }
  }, function(error) {
    console.error("Query failed:", error);
    alert("Query failed: " + error);
    document.getElementById('search-error').style.display = '';
    document.getElementById('search-error').innerText = '' + error;
  }).catch(console.error);
});
