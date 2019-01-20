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

var indices = {'required': {
                 'temporal': [],
                 'geospatial': [],
                 'generic': []},
               'desired': {
                 'temporal': [],
                 'geospatial': [],
                 'generic': []}}

var n_temporal = 0;
var n_geospatial = 0;
var n_generic = 0;

var maps = [];
var map_sources = [];
var draws = [];

function variableChange(select_id, div_id) {
    var select = document.getElementById(select_id);
    window["add_" + select.options[select.selectedIndex].value](div_id);
}

function addInteraction(index) {
    map_sources[index-1].clear();
    var value = document.getElementById('shape-type-' + index).value;
    if (value !== 'None') {
      var geometryFunction;
      if (value === 'bb-' + index) {
        geometryFunction = ol.interaction.Draw.createBox();
      }
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
 }

function add_temporal(div_id) {
    n_temporal += 1;
    indices[div_id]['temporal'].push(n_temporal)
    var index = n_temporal;

    var variables_div = document.getElementById(div_id);

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

function add_geospatial(div_id) {
    n_geospatial += 1;
    indices[div_id]['geospatial'].push(n_geospatial);
    var index = n_geospatial;

    var variables_div = document.getElementById(div_id);

    var _title = document.createElement('h5');
    _title.innerHTML = 'Geospatial Bounds';
    variables_div.appendChild(_title);

    var shape_div = document.createElement('div');
    shape_div.setAttribute('class', 'd-flex align-items-center');
    var shape_select = document.createElement('select');
    shape_select.setAttribute('id', 'shape-type-' + index);
    shape_select.setAttribute('class', 'mb-2');
    shape_select.setAttribute('onChange', onclick='addInteraction(' + index + ');');
    var shape_rectangle = document.createElement('option');
    shape_rectangle.setAttribute('value', 'bb-' + index);
    shape_rectangle.setAttribute('selected', 'selected');
    shape_rectangle.innerHTML = 'Bounding Box';
    shape_select.appendChild(shape_rectangle);
    var shape_circle = document.createElement('option');
    shape_circle.setAttribute('value', 'circle-' + index);
    shape_circle.innerHTML = 'Circle';
    shape_select.appendChild(shape_circle);
    shape_div.appendChild(shape_select);

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
      var shape = document.getElementById('shape-type-' + index).value;
      var bounds = document.getElementById('bounds-' + index);
      if (shape === 'bb-' + index) {
        var coord = geometry.clone().transform('EPSG:3857', 'EPSG:4326').getCoordinates()[0];
        var top_left_lat = coord[3][1];
        var top_left_lon = coord[3][0];
        var bottom_right_lat = coord[1][1];
        var bottom_right_lon = coord[1][0];

        var text = 'Top Left: <code>' + ol.coordinate.toStringHDMS([top_left_lon, top_left_lat]) + '</code> &nbsp;&nbsp;&nbsp;';
        text += '&nbsp;&nbsp;&nbsp;Bottom Right: <code>' + ol.coordinate.toStringHDMS([bottom_right_lon, bottom_right_lat]) + '</code>';
        bounds.innerHTML = text;
      } else {
        var new_geometry = geometry.clone().transform('EPSG:3857', 'EPSG:4326');
        var center_lat = new_geometry.getCenter()[1];
        var center_lon = new_geometry.getCenter()[0];
        var radius = new_geometry.getRadius();

        var text = 'Center: ' + ol.coordinate.toStringHDMS([center_lon, center_lat]) + ' | ';
        text += 'Radius: ' + radius;
        bounds.innerHTML = text;
      }
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

function add_generic(div_id) {
    n_generic += 1;
    indices[div_id]['generic'].push(n_generic);
    var index = n_generic;

    var variables_div = document.getElementById(div_id);

    var _title = document.createElement('h5')
    _title.innerHTML = 'Generic Column'
    variables_div.appendChild(_title)

    var _div_name_1 = document.createElement('div')
    _div_name_1.setAttribute('class', 'form-row')
    var _div_name_2 = document.createElement('div')
    _div_name_2.setAttribute('class', 'form-group col-md-8')
    var _label_name = document.createElement('label')
    _label_name.setAttribute('for', 'name-' + index)
    _label_name.innerHTML = 'Column Name:'
    _div_name_2.appendChild(_label_name)
    var _input_name = document.createElement('input')
    _input_name.setAttribute('class', 'form-control')
    _input_name.setAttribute('id', 'name-' + index)
    _div_name_2.appendChild(_input_name)
    _div_name_1.appendChild(_div_name_2)
    variables_div.appendChild(_div_name_1)

    var _div_types = document.createElement('div')
    _div_types.setAttribute('class', 'form-group col-md-8')

    var _div_integer = document.createElement('div')
    _div_integer.setAttribute('class', 'form-check form-check-inline')
    var _input_integer = document.createElement('input')
    _input_integer.setAttribute('class', 'form-check-input')
    _input_integer.setAttribute('type', 'checkbox')
    _input_integer.setAttribute('id', 'type-integer-' + index)
    _div_integer.appendChild(_input_integer)
    var _label_integer = document.createElement('label')
    _label_integer.setAttribute('class', 'form-check-label')
    _label_integer.setAttribute('for', 'type-integer-' + index)
    _label_integer.innerHTML = 'Integer'
    _div_integer.appendChild(_label_integer)

    _div_types.appendChild(_div_integer)

    var _div_float = document.createElement('div')
    _div_float.setAttribute('class', 'form-check form-check-inline')
    var _input_float = document.createElement('input')
    _input_float.setAttribute('class', 'form-check-input')
    _input_float.setAttribute('type', 'checkbox')
    _input_float.setAttribute('id', 'type-float-' + index)
    _div_float.appendChild(_input_float)
    var _label_float = document.createElement('label')
    _label_float.setAttribute('class', 'form-check-label')
    _label_float.setAttribute('for', 'type-float-' + index)
    _label_float.innerHTML = 'Float'
    _div_float.appendChild(_label_float)

    _div_types.appendChild(_div_float)

    var _div_bool = document.createElement('div')
    _div_bool.setAttribute('class', 'form-check form-check-inline')
    var _input_bool = document.createElement('input')
    _input_bool.setAttribute('class', 'form-check-input')
    _input_bool.setAttribute('type', 'checkbox')
    _input_bool.setAttribute('id', 'type-bool-' + index)
    _div_bool.appendChild(_input_bool)
    var _label_bool = document.createElement('label')
    _label_bool.setAttribute('class', 'form-check-label')
    _label_bool.setAttribute('for', 'type-bool-' + index)
    _label_bool.innerHTML = 'Boolean'
    _div_bool.appendChild(_label_bool)

    _div_types.appendChild(_div_bool)

    var _div_text = document.createElement('div')
    _div_text.setAttribute('class', 'form-check form-check-inline')
    var _input_text = document.createElement('input')
    _input_text.setAttribute('class', 'form-check-input')
    _input_text.setAttribute('type', 'checkbox')
    _input_text.setAttribute('id', 'type-text-' + index)
    _div_text.appendChild(_input_text)
    var _label_text = document.createElement('label')
    _label_text.setAttribute('class', 'form-check-label')
    _label_text.setAttribute('for', 'type-text-' + index)
    _label_text.innerHTML = 'Text'
    _div_text.appendChild(_label_text)

    _div_types.appendChild(_div_text)

    var _div_datetime = document.createElement('div')
    _div_datetime.setAttribute('class', 'form-check form-check-inline')
    var _input_datetime = document.createElement('input')
    _input_datetime.setAttribute('class', 'form-check-input')
    _input_datetime.setAttribute('type', 'checkbox')
    _input_datetime.setAttribute('id', 'type-datetime-' + index)
    _div_datetime.appendChild(_input_datetime)
    var _label_datetime = document.createElement('label')
    _label_datetime.setAttribute('class', 'form-check-label')
    _label_datetime.setAttribute('for', 'type-datetime-' + index)
    _label_datetime.innerHTML = 'Date/Time'
    _div_datetime.appendChild(_label_datetime)

    _div_types.appendChild(_div_datetime)

    var _div_spatial = document.createElement('div')
    _div_spatial.setAttribute('class', 'form-check form-check-inline')
    var _input_spatial = document.createElement('input')
    _input_spatial.setAttribute('class', 'form-check-input')
    _input_spatial.setAttribute('type', 'checkbox')
    _input_spatial.setAttribute('id', 'type-spatial-' + index)
    _div_spatial.appendChild(_input_spatial)
    var _label_spatial = document.createElement('label')
    _label_spatial.setAttribute('class', 'form-check-label')
    _label_spatial.setAttribute('for', 'type-spatial-' + index)
    _label_spatial.innerHTML = 'Spatial'
    _div_spatial.appendChild(_label_spatial)

    _div_types.appendChild(_div_spatial)

    var _div_phone = document.createElement('div')
    _div_phone.setAttribute('class', 'form-check form-check-inline')
    var _input_phone = document.createElement('input')
    _input_phone.setAttribute('class', 'form-check-input')
    _input_phone.setAttribute('type', 'checkbox')
    _input_phone.setAttribute('id', 'type-phone-' + index)
    _div_phone.appendChild(_input_phone)
    var _label_phone = document.createElement('label')
    _label_phone.setAttribute('class', 'form-check-label')
    _label_phone.setAttribute('for', 'type-phone-' + index)
    _label_phone.innerHTML = 'Phone Number'
    _div_phone.appendChild(_label_phone)

    _div_types.appendChild(_div_phone)

    variables_div.appendChild(_div_types)
}

var search_results = [];
var search_data = null;

function submitAugmentationForm(i)
{
    var data_augment = {};
    data_augment.data = search_data;
    data_augment.task = search_results[i];
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

    search.query.dataset = {};
    var keywords = document.getElementById('keywords').value;
    if(keywords) {
      search.query.dataset.about = keywords;
    }
    var names = document.getElementById('names').value;
    names = names.split(/[ ,+]+/);
    if(names.length == 1 && names[0] === '') {
      names = [];
    } else {
      search.query.dataset.name = names;
    }
    var description = document.getElementById('description').value;
    description = description.split(/[ ,+]+/);
    if(description.length == 1 && description[0] === '') {
      description = [];
    } else {
      search.query.dataset.description = description;
    }

    for(key in indices) {
      search.query[key + '_variables'] = []
      if(indices[key]['temporal'].length > 0) {
        for (var i = 0; i < indices[key]['temporal'].length; i++) {
            var index = indices[key]['temporal'][i];
            var start = document.getElementById('start-' + index).value;
            var end = document.getElementById('end-' + index).value;
            if(start || end) {
                var variable = {'type': 'temporal_entity'};
                if(start) {
                    variable['start'] = start;
                }
                if(end) {
                    variable['end'] = end;
                }
                search.query[key + '_variables'].push(variable)
            }
        }
      }
      if(indices[key]['geospatial'].length > 0) {
        for (var i = 0; i < indices[key]['geospatial'].length; i++) {
            var index = indices[key]['geospatial'][i];
            var variable = {'type': 'geospatial_entity'};
            var features = map_sources[index-1].getFeatures();
            if (features.length > 0) {
                var geometry = features[0].getGeometry();
                var shape = document.getElementById('shape-type-' + index).value;
                if (shape === 'bb-' + index) {
                  var coord = geometry.clone().transform('EPSG:3857', 'EPSG:4326').getCoordinates()[0];
                  var top_left_lat = coord[3][1];
                  var top_left_lon = coord[3][0];
                  var bottom_right_lat = coord[1][1];
                  var bottom_right_lon = coord[1][0];

                  variable['bounding_box'] = {'latitude1': top_left_lat,
                                              'latitude2': bottom_right_lat,
                                              'longitude1': top_left_lon,
                                              'longitude2': bottom_right_lon}
                } else {
                  var new_geometry = geometry.clone().transform('EPSG:3857', 'EPSG:4326');
                  var center_lat = new_geometry.getCenter()[1];
                  var center_lon = new_geometry.getCenter()[0];
                  var radius = new_geometry.getRadius();

                  variable['circle'] = {'latitude': center_lat,
                                        'longitude': center_lon,
                                        'radius': radius}
                }
                search.query[key + '_variables'].push(variable)
            }
        }
      }
      if(indices[key]['generic'].length > 0) {
        for (var i = 0; i < indices[key]['generic'].length; i++) {
            var index = indices[key]['generic'][i];
            var column_names = document.getElementById('name-' + index).value;
            column_names = column_names.split(/[ ,+]+/);
            if(column_names.length == 1 && column_names[0] === '') {
                column_names = [];
            }
            var str_types = [];
            var sem_types = [];
            if(document.getElementById('type-integer-' + index).checked) {
                str_types.push('http://schema.org/Integer');
            }
            if(document.getElementById('type-float-' + index).checked) {
                str_types.push('http://schema.org/Float');
            }
            if(document.getElementById('type-bool-' + index).checked) {
                sem_types.push('http://schema.org/Boolean');
            }
            if(document.getElementById('type-text-' + index).checked) {
                str_types.push('http://schema.org/Text');
            }
            if(document.getElementById('type-datetime-' + index).checked) {
                sem_types.push('http://schema.org/DateTime');
            }
            if(document.getElementById('type-spatial-' + index).checked) {
                sem_types.push('http://schema.org/latitude');
                sem_types.push('http://schema.org/longitude');
            }
            if(document.getElementById('type-phone-' + index).checked) {
                sem_types.push('https://metadata.datadrivendiscovery.org/types/PhoneNumber');
            }
            if(column_names.length > 0 || str_types.length > 0 || sem_types.length > 0) {
                var variable = {'type': 'generic_entity'};
                if(column_names.length > 0) {
                    variable['variable_name'] = column_names;
                }
                if(str_types.length > 0) {
                    variable['variable_syntactic_type'] = str_types;
                }
                if(sem_types.length > 0) {
                    variable['variable_semantic_type'] = sem_types;
                }
                search.query[key + '_variables'].push(variable)
            }
        }
      }
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

      var join_info = '';
      if((data.join_columns) && (data.join_columns.length > 0)) {
        var columns_info = '';
        for(var j = 0; j < data.join_columns.length; j++) {
            var column = data.join_columns[j];
            columns_info += '      <li class="list-group-item"><small><em>' + column[0] + '</em> and <em>' + column[1] + '</em></small></li>';
        }

        join_info = (
            '    <hr>' +
            '    <p class="card-text"><em>Join Information</em></p>' +
            '    <ul class="list-group text-muted">' +
            '      <li class="list-group-item"><small>Score: ' + data.score + '</small></li>' + columns_info +
            '    </ul>'
        );
      }

      var union_info = '';
      if((data.union_columns) && (data.union_columns.length > 0)) {
        var columns_info = '';
        for(var j = 0; j < data.union_columns.length; j++) {
            var column = data.union_columns[j];
            columns_info += '      <li class="list-group-item"><small><em>' + column[0] + '</em> and <em>' + column[1] + '</em></small></li>';
        }

        union_info = (
            '    <hr>' +
            '    <p class="card-text"><em>Union Information</em></p>' +
            '    <ul class="list-group text-muted">' +
            '      <li class="list-group-item"><small>Score: ' + data.score + '</small></li>' + columns_info +
            '    </ul>'
        );
      }

      var augment_button = '';
      if(search.data) {
        augment_button = ('<a href="javascript: submitAugmentationForm('+ i + ')" ' +
                          'class="btn btn-sm btn-outline-secondary">Augment</a>');
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
        // augment_button +
        '      </div>' +
        '      <small class="text-muted">' + (data.metadata.size?formatSize(data.metadata.size):'unknown size') + '</small>' +
        '    </div>' + join_info + union_info +
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
