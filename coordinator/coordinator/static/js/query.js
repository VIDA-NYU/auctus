function getCookie(name) {
  var r = document.cookie.match("\\b" + name + "=([^;]*)\\b");
  return r ? r[1] : undefined;
}

function postSearchForm(url='', data={}) {
  var formData = new FormData();
  if(data.data) {
    formData.append('data', data.data);
  }
  formData.append('query', JSON.stringify(data.query));
  return fetch(
    url + '?_xsrf=' + encodeURIComponent(getCookie('_xsrf')),
    {
      mode: 'cors',
      cache: 'no-cache',
      method: 'POST',
      body: formData
    }
  ).then(function(response) {
    if(response.status != 200) {
      throw "Status " + response.status;
    }
    return response.json();
  });
}

function postAugmentForm(url='', data={}) {
  var formData = new FormData();
  formData.append('data', data.data);
  formData.append('task', JSON.stringify(data.task));
  return fetch(
    url + '?_xsrf=' + encodeURIComponent(getCookie('_xsrf')),
    {
      mode: 'cors',
      cache: 'no-cache',
      method: 'POST',
      body: formData
    }
  ).then(function(response) {
    if(response.status != 200) {
      throw "Status " + response.status;
    }
    return response.blob();
  });
}

var indices = {'temporal': [],
               'geospatial': []}

var n_temporal = 0;
var n_geospatial = 0;

var maps = [];
var map_sources = [];
var draws = [];

function variableChange(select_id) {
    var select = document.getElementById(select_id);
    window["add_" + select.options[select.selectedIndex].value]();
}

function addInteraction(index) {
    map_sources[index-1].clear();
      var geometryFunction;
      geometryFunction = ol.interaction.Draw.createBox();
      value = 'Circle';
      var draw = new ol.interaction.Draw({
        source: map_sources[index-1],
        type: value,
        geometryFunction: geometryFunction,
        condition: function(e) {
          // when the point's button is 1(leftclick), allows drawing
          if (e.pointerEvent.buttons === 1) {
            return true;
          } else {
            return false;
          }
        }
      });
      draw.on('drawstart', function (e) {
        map_sources[index-1].clear();
      });
      if (draws.length >= index) {
        maps[index-1].removeInteraction(draws[index-1]);
        draws[index-1] = draw;
      } else {
        draws.push(draw);
      }
      maps[index-1].addInteraction(draws[index-1]);
 }

function add_temporal() {
    n_temporal += 1;
    indices['temporal'].push(n_temporal)
    var index = n_temporal;

    var variables_div = document.getElementById('variables');

    var _title = document.createElement('h5')
    _title.innerHTML = 'Temporal Information'
    variables_div.appendChild(_title)

    var _div_start_1 = document.createElement('div')
    _div_start_1.setAttribute('class', 'form-row')
    var _div_start_2 = document.createElement('div')
    _div_start_2.setAttribute('class', 'form-group col-md-8')
    var _label_start = document.createElement('label')
    _label_start.setAttribute('for', 'start-' + index)
    _label_start.innerHTML = 'From:'
    _div_start_2.appendChild(_label_start)
    var _input_start = document.createElement('input')
    _input_start.setAttribute('class', 'form-control')
    _input_start.setAttribute('id', 'start-' + index)
    _div_start_2.appendChild(_input_start)
    _div_start_1.appendChild(_div_start_2)
    variables_div.appendChild(_div_start_1)

    var _div_end_1 = document.createElement('div')
    _div_end_1.setAttribute('class', 'form-row')
    var _div_end_2 = document.createElement('div')
    _div_end_2.setAttribute('class', 'form-group col-md-8')
    var _label_end = document.createElement('label')
    _label_end.setAttribute('for', 'end-' + index)
    _label_end.innerHTML = 'To:'
    _div_end_2.appendChild(_label_end)
    var _input_end = document.createElement('input')
    _input_end.setAttribute('class', 'form-control')
    _input_end.setAttribute('id', 'end-' + index)
    _div_end_2.appendChild(_input_end)
    _div_end_1.appendChild(_div_end_2)
    variables_div.appendChild(_div_end_1)
}

function add_geospatial() {
    n_geospatial += 1;
    indices['geospatial'].push(n_geospatial);
    var index = n_geospatial;

    var variables_div = document.getElementById('variables');

    var _title = document.createElement('h5');
    _title.innerHTML = 'Geospatial Bounds';
    variables_div.appendChild(_title);

    var shape_div = document.createElement('div');
    shape_div.setAttribute('class', 'd-flex align-items-center');

    var bounds = document.createElement('p');
    bounds.setAttribute('id', 'bounds-' + index);
    bounds.setAttribute('class', 'ml-auto mb-2 text-right');
    bounds.setAttribute('style', 'font-size: 11px;');
    shape_div.appendChild(bounds);

    variables_div.appendChild(shape_div);

    var map_div = document.createElement('div');
    map_div.setAttribute('id', 'map-' + index);
    map_div.setAttribute('class', 'map mb-1');
    variables_div.appendChild(map_div);

    var info = document.createElement('p');
    info.setAttribute('class', 'mb-3');
    info.setAttribute('style', 'font-size: 11px;');
    info.innerHTML = 'Right-click to clear selection.';
    variables_div.appendChild(info);

    var raster = new ol.layer.Tile({
      source: new ol.source.OSM()
    });

    var source = new ol.source.Vector({wrapX: false});
    map_sources.push(source);

    var vector = new ol.layer.Vector({
      source: map_sources[index-1]
    });

    map_sources[index-1].on('addfeature', function(evt) {
      var geometry = evt.feature.getGeometry();
      var bounds = document.getElementById('bounds-' + index);

      var coord = geometry.clone().transform('EPSG:3857', 'EPSG:4326').getCoordinates()[0];
      var top_left_lat = coord[3][1];
      var top_left_lon = coord[3][0];
      var bottom_right_lat = coord[1][1];
      var bottom_right_lon = coord[1][0];

      var text = 'Top Left: <code>' + ol.coordinate.toStringHDMS([top_left_lon, top_left_lat]) + '</code> &nbsp;&nbsp;&nbsp;';
      text += '&nbsp;&nbsp;&nbsp;Bottom Right: <code>' + ol.coordinate.toStringHDMS([bottom_right_lon, bottom_right_lat]) + '</code>';
      bounds.innerHTML = text;
    });

    var map = new ol.Map({
      layers: [raster, vector],
      target: 'map-' + index,
      view: new ol.View({
        projection: 'EPSG:3857',
        center: ol.proj.fromLonLat([-73.986579, 40.6942036], 'EPSG:3857'), // Tandon
        zoom: 12
      })
    });
    maps.push(map);

    maps[index-1].getViewport().addEventListener('contextmenu', function (evt) {
      document.getElementById('bounds-' + index).innerHTML = '';
      map_sources[index-1].clear();
    })

    addInteraction(index);
}

var search_results = [];
var search_data = null;

function submitAugmentationForm(i)
{
    var data_augment = {};
    data_augment.data = search_data;
    data_augment.task = search_results[i];

    // checking which pairs to submit
    var active_pairs = [];
    if((data_augment.task.join_columns) && (data_augment.task.join_columns.length > 0)) {
        for(var j=0; j < data_augment.task.join_columns.length; j++) {
            if(document.getElementById('pair-join-' + i + '-' + j).getAttribute('class').includes('active')) {
                active_pairs.push(data_augment.task.join_columns[j])
            }
        }
        if(active_pairs.length > 0) {
            data_augment.task.join_columns = active_pairs;
        }
    } else if ((data_augment.task.union_columns) && (data_augment.task.union_columns.length > 0)) {
        for(var j=0; j < data_augment.task.union_columns.length; j++) {
            if(document.getElementById('pair-union-' + i + '-' + j).getAttribute('class').includes('active')) {
                active_pairs.push(data_augment.task.union_columns[j])
            }
        }
        if(active_pairs.length > 0) {
            data_augment.task.union_columns = active_pairs;
        }
    }

    postAugmentForm(QUERY_HOST + '/augment', data_augment)
    .then(function(zipFile) {
        // The actual download
        var blob = zipFile;
        var link = document.createElement('a');
        link.href = window.URL.createObjectURL(blob);
        link.download = 'augmentation.zip'
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    });
}

function changePairStatus(type_, result_id, pair_id) {
    var pair = document.getElementById('pair-' + type_ + '-' + result_id + '-' + pair_id);
    var pair_class = pair.getAttribute('class');
    if(pair_class.includes('active')) {
        pair.setAttribute('class', 'list-group-item list-group-item-action');
    } else {
        pair.setAttribute('class', 'list-group-item list-group-item-action active');
    }
}

function toggleAugmentation(id) {
    var aug = document.getElementById('aug-info-' + id);
    if(aug.style.display == 'block') {
        aug.style.display = 'none';
        document.getElementById('arrow-aug-' + id).innerHTML = '&#x25BC;';
    }
    else {
        aug.style.display = 'block';
        document.getElementById('arrow-aug-' + id).innerHTML = '&#x25B2;';
    }
}

function getAugmentationInfoHTML(left_columns_names, right_metadata, right_columns, score, result_id, type_) {
    var columns_info = '';
    for(var j = 0; j < left_columns_names.length; j++) {
        var left_column = left_columns_names[j].join(", ");
        var right_column = right_columns[j]
          .map(function(i) { return right_metadata.columns[i].name; })
          .join(", ");
        columns_info += (
            '<a href="javascript: changePairStatus(\'' + type_ + '\',' + result_id + ',' + j + ');" class="list-group-item list-group-item-action" ' +
            '  id="pair-' + type_ + '-' + result_id + '-' + j + '">' +
            '  <small><em>' + left_column + '</em> and <em>' + right_column + '</em></small>' +
            '</a>'
        );
    }

    info = (
        '    <hr>' +
        '    <div class="d-flex justify-content-between align-items-center">' +
        '      <p class="mb-0">Augmentation Information</p>' +
        '      <button type="button" class="btn btn-outline-secondary" onclick="javascript: toggleAugmentation(' + result_id + ')" id="arrow-aug-' + result_id + '">&#x25BC;</button>' +
        '    </div>' +
        '    <div id="aug-info-' + result_id + '" style="display:none;">' +
        '      <p class="card-text"><small>Type: <em>' + type_.charAt(0).toUpperCase() + type_.substr(1) + '</em></small></p>' +
        '      <div class="list-group text-muted">' +
        '        <a class="list-group-item"><small>Score: ' + score + '</small></li>' + columns_info +
        '      </div>' +
//        '      <div class="btn-group mt-3">' +
//        '        <a href="javascript: submitAugmentationForm('+ result_id + ')" class="btn btn-sm btn-outline-secondary">Augment</a>' +
//        '      </div>' +
        '    </div>'
    );

    return info;
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

function readFile(files, callback) {
  if(files.length) {
    callback(files[0].name, files[0]);
  } else {
    callback('', null);
  }
}

document.getElementById('search-form').addEventListener('submit', function(e) {
  e.preventDefault();

  var file_input = document.getElementById('file');
  readFile(file_input.files, function(name, result) {

    var search = {};

    // Data
    search.data = result;

    // Query
    search.query = {};

    search.query.keywords = [];
    var keywords = document.getElementById('keywords').value;
    keywords = keywords.split(/[ ,+]+/);
    if(keywords.length == 1 && keywords[0] === '') {
      keywords = [];
    } else {
      search.query.keywords = keywords;
    }

    search.query.variables = []

    for(key in indices) {
      if(indices[key].length > 0) {
        for (var i = 0; i < indices[key].length; i++) {
            var index = indices[key][i];
            if (key == 'temporal') {
                var start = document.getElementById('start-' + index).value;
                var end = document.getElementById('end-' + index).value;
                if(start || end) {
                    var variable = {'type': 'temporal_variable'};
                    if(start) {
                        variable['start'] = start;
                    }
                    if(end) {
                        variable['end'] = end;
                    }
                    search.query.variables.push(variable)
                }
            } else {
                var variable = {'type': 'geospatial_variable'};
                var features = map_sources[index-1].getFeatures();
                if (features.length > 0) {
                    var geometry = features[0].getGeometry();
                    var coord = geometry.clone().transform('EPSG:3857', 'EPSG:4326').getCoordinates()[0];
                    var top_left_lat = coord[3][1];
                    var top_left_lon = coord[3][0];
                    var bottom_right_lat = coord[1][1];
                    var bottom_right_lon = coord[1][0];

                    variable['latitude1'] = top_left_lat;
                    variable['latitude2'] = bottom_right_lat;
                    variable['longitude1'] = top_left_lon;
                    variable['longitude2'] = bottom_right_lon;

                    search.query.variables.push(variable);
                }
            }
        }
      }
    }

    // search columns

    var search_columns = document.getElementById('search_columns').value;
    search_columns = search_columns.split(/[ ,+]+/);
    if(search_columns.length == 1 && search_columns[0] === '') {
        search_columns = [];
    } else {
       for (var i = 0; i < search_columns.length; i++) {
           search_columns[i] = parseInt(search_columns[i]);
       }
       var variable = {'type': 'tabular_variable'};
       variable['columns'] = search_columns
       variable['relationship'] = 'contains'
       search.query.variables.push(variable);
    }

    var results_div = document.getElementById('results');
    // cleaning previous results
    while (results_div.firstChild) {
        results_div.removeChild(results_div.firstChild);
    }
    document.getElementById('search-error').style.display = 'none';
    search_results = [];
    search_data = null;

    // showing progress icon
    document.getElementById('processing').style.display = 'block';

    console.log("Searching:", search);
    postSearchForm(QUERY_HOST + '/search', search)
    .then(function(result) {
    console.log("Got " + result.results.length + " results");
    console.log("Results:", result.results);
    results_div.innerHTML = '';
    document.getElementById('processing').style.display = 'none';
    search_results = result.results;
    search_data = search.data;
    if((search_results.length > 0) && (search.data)) {
        var div_title = document.createElement('div');
        div_title.setAttribute('class', 'container mb-5')
        var title = document.createElement('h6');
        title.innerHTML = 'Results for ';
        title.innerHTML += name;
        div_title.appendChild(title);
        results_div.appendChild(div_title);
    }
    for(var i = 0; i < search_results.length; ++i) {
      var elem = document.createElement('div');
      var data = search_results[i];
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

      var aug_info = '';
      if ((data.augmentation) && data.augmentation.type != 'none') {
         aug_info = getAugmentationInfoHTML(
            data.augmentation.left_columns_names,
            data.metadata,
            data.augmentation.right_columns,
            data.score,
            i,
            data.augmentation.type);
      }

      elem.innerHTML = (
        '<div class="card mb-4 shadow-sm">' +
        '  <div class="card-body">' +
        '    <p class="card-text">' + (data.metadata.name || data.id) + '</p>' +
        '    <p class="card-text">' + description + '</p>' +
        '    <div class="d-flex justify-content-between align-items-center mb-3">' +
        '      <div class="btn-group">' +
        '        <a href="/dataset/' + data.id + '" class="btn btn-sm btn-outline-secondary">View</a>' +
        '        <a href="' + QUERY_HOST + '/download/' + data.id + '" class="btn btn-sm btn-outline-secondary">Download</a>' +
        '      </div>' +
        '      <small class="text-muted">' + (data.metadata.size?formatSize(data.metadata.size):'unknown size') + '</small>' +
        '    </div>' + aug_info +
        '  </div>' +
        '</div>'
      );
      results_div.appendChild(elem);
    }
    if(search_results.length == 0) {
      document.getElementById('search-error').style.display = '';
      document.getElementById('search-error').innerText = "No results";
    }
    }, function(error) {
    document.getElementById('processing').style.display = 'none';
    console.error("Query failed:", error);
    alert("Query failed: " + error);
    document.getElementById('search-error').style.display = '';
    document.getElementById('search-error').innerText = '' + error;
    }).catch(console.error);

    });

});
