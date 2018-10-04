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
      mode: 'same-origin'
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
      var elem = document.createElement('li');
      if(result.recent_discoveries[i][2]) {
        elem.innerHTML = linkDataset(result.recent_discoveries[i][0]);
      } else {
        elem.innerHTML =
          linkDataset(result.recent_discoveries[i][0]) +
          ' <span style="font-family: monospace;">' +
          result.recent_discoveries[i][1] + '</span>';
      }
      recent_discoveries.appendChild(elem);
    }
    if(result.recent_discoveries.length == 0) {
      var elem = document.createElement('li');
      elem.innerHTML = "No recent discoveries";
      recent_discoveries.appendChild(elem);
    }
  });
}

loadStatus();
setInterval(loadStatus, 2000);
