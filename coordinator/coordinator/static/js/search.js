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
               'geospatial': []};

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

var _temporal_template = loadTemplate('variable-temporal-template');
function add_temporal() {
  n_temporal += 1;
  indices['temporal'].push(n_temporal)
  var index = n_temporal;

  var variables_div = document.getElementById('variables');
  _temporal_template(variables_div, {index});
}

var _geospatial_template = loadTemplate('variable-geospatial-template');
function add_geospatial() {
  n_geospatial += 1;
  indices['geospatial'].push(n_geospatial);
  var index = n_geospatial;

  var variables_div = document.getElementById('variables');
  _geospatial_template(variables_div, {index});

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

var _augmentation_column_template = loadTemplate('augmentation-column-template');
var _augmentation_template = loadTemplate('augmentation-template');
function getAugmentationInfoHTML(left_columns_names, right_metadata, right_columns, score, result_id, type) {
  var columns_info = '';
  for(var j = 0; j < left_columns_names.length; j++) {
    var left_column = left_columns_names[j].join(", ");
    var right_column = right_columns[j]
      .map(function(i) { return right_metadata.columns[i].name; })
      .join(", ");
    columns_info += _augmentation_column_template(
      null,
      {
        type,
        id: result_id,
        left_column,
        right_column,
        j,
      },
    );
  }

  info = _augmentation_template(
    null,
    {
      type,
      id: result_id,
      score,
      columns_info,
    },
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

var _search_result_template = loadTemplate('search-result-template');
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
    keywords = keywords.split(',');
    if(keywords.length == 1 && keywords[0] === '') {
      keywords = [];
    } else {
      search.query.keywords = keywords;
    }

    var source =
      [].filter.call(
        document.getElementsByClassName('source-checkbox'),
        function(cb) { return cb.checked; }
      ).map(function(cb) { return cb.value; });
    console.log("source:", search.query.source);
    if(source.length > 0) {
      search.query.source = source;
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
    postSearchForm(QUERY_HOST + '/search?_parse_sample=1', search)
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
        var data = search_results[i];
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
             data.augmentation.type
           );
        }

        var badges = '';
        if(data.metadata.spatial_coverage) {
          badges += ' <span class="badge badge-primary badge-pill">spatial</span>';
        }
        if(data.metadata.columns.some(function(c) { return c['semantic_types'].indexOf('http://schema.org/DateTime') != -1; })) {
          badges += ' <span class="badge badge-secondary badge-pill">temporal</span>';
        }

        var table = '<thead><tr>';
        for(var j = 0; j < data.metadata.columns.length; ++j) {
          table += '<th>' + data.metadata.columns[j].name + '</th>';
        }
        table += '</tr><tr>';
        for(var j = 0; j < data.metadata.columns.length; ++j) {
          var st_type = data.metadata.columns[j].structural_type;
          var pos = st_type.lastIndexOf('/');
          if(pos != -1) {
            st_type = st_type.substring(pos + 1);
          }
          table += '<th><span class="badge badge-primary badge-pill">' + st_type + '</span></th>';
        }
        table += '</tr><tr>';
        for(var j = 0; j < data.metadata.columns.length; ++j) {
          var sem_types = data.metadata.columns[j].semantic_types;
          table += '<th>';
          for(var k = 0; k < sem_types.length; ++k) {
            var sem_type = sem_types[k];
            var pos = sem_type.lastIndexOf('/');
            if(pos != -1) {
              sem_type = sem_type.substring(pos + 1);
            }
            table += '<span class="badge badge-info badge-pill">' + sem_type + '</span>\n';
          }
          table += '</th>';
        }
        table += '</tr></thead><tbody>';
        if(data.sample) {
          // skip first line, it's the header
          for(var j = 1; j < Math.min(4, data.sample.length); ++j) {
            table += '<tr>';
            for(var k = 0; k < data.sample[j].length; ++k) {
              table += '<td>' + data.sample[j][k] + '</td>';
            }
            table += '</tr>';
          }
        }
        table += '</tbody>';

        _search_result_template(
          results_div,
          {
            name: data.metadata.name || data.id,
            description,
            badges,
            id: data.id,
            size: data.metadata.size ? formatSize(data.metadata.size) : 'unknown size',
            table,
            aug_info,
          },
        );
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
