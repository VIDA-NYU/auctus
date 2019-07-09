if(!Object.entries) {
  Object.entries = function(obj) {
    var ownProps = Object.keys(obj),
      i = ownProps.length,
      resArray = new Array(i); // preallocate the Array
    while(i--) {
      resArray[i] = [ownProps[i], obj[ownProps[i]]];
    }

    return resArray;
  };
}

function getCookie(name) {
  var r = document.cookie.match("\\b" + name + "=([^;]*)\\b");
  return r ? r[1] : undefined;
}

function getJSON(url='', args) {
  if(args) {
    args = '&' + encodeGetParams(args);
  } else {
    args = '';
  }
  return fetch(
    url + '?_xsrf=' + encodeURIComponent(getCookie('_xsrf')) + args,
    {
      credentials: 'same-origin',
      mode: 'cors'
    }
  ).then(function(response) {
    if(response.status != 200) {
      throw "Status " + response.status;
    }
    return response.json();
  });
}

function linkDataset(dataset_id) {
  dataset_id = escape(dataset_id);
  return '<a class="dataset" href="/dataset/' + dataset_id + '">' + dataset_id + '</a>';
}

function loadStatus() {
  getJSON('/status')
  .then(function(result) {
    var recent_discoveries = document.getElementById('recent_discoveries');
    recent_discoveries.innerHTML = '';
    for(var i = 0; i < result.recent_discoveries.length; ++i) {
      var elem;
      var record = result.recent_discoveries[i];
      if(record.profiled) {
        elem = document.createElement('a');
        elem.setAttribute('href', '/dataset/' + escape(record.id));
        elem.className = 'list-group-item list-group-item-action flex-column align-items-start';
        elem.innerHTML =
          '<div class="d-flex w-100 justify-content-between">' +
          '  <h5 class="mb-1">' + (record.name || record.id) + '</h5>' +
          '  <small>' + record.profiled + '</small>' +
          '</div>' +
          //'<p class="mb-1">More info?</p>' +
          '<small>' + record.discoverer + '</small>';
      } else {
        elem = document.createElement('div');
        elem.className = 'list-group-item flex-column align-items-start list-group-item-secondary';
        elem.innerHTML =
          '<div class="d-flex w-100 justify-content-between">' +
          '  <h5 class="mb-1">' + (record.name || record.id) + '</h5>' +
          '  <small class="text-muted">' + record.discovered + '</small>' +
          '</div>' +
          '<p class="mb-1">Processing...</p>' +
          '<small>' + record.discoverer + '</small>';
      }
      recent_discoveries.appendChild(elem);
    }
    if(result.recent_discoveries.length == 0) {
      var elem = document.createElement('div');
      elem.className = 'list-group-item flex-column align-items-start list-group-item-secondary';
      elem.innerHTML = "<h5>No recent discoveries</h5>";
      recent_discoveries.appendChild(elem);
    }
    var sources = document.getElementById('sources');
    sources.innerHTML = '';
    sources_counts = Object.entries(result.sources_counts);
    sources_counts.sort(function(a, b) {
      if(a[1] < b[1]) {
        return 1;
      } else if(a[1] > b[1]) {
        return -1;
      } else {
        return 0;
      }
    });
    for(var i = 0; i < sources_counts.length; ++i) {
      var elem = document.createElement('li');
      elem.innerHTML =
        sources_counts[i][1] +
        ' datasets from ' +
        sources_counts[i][0];
        sources.appendChild(elem);
    }
  })
  .catch(console.error);
}

loadStatus();
setInterval(loadStatus, 2000);
