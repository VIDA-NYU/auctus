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
                 'temporal': 0,
                 'geospatial_bb': 0,
                 'geospatial_circle': 0,
                 'generic': 0},
               'desired': {
                 'temporal': 0,
                 'geospatial_bb': 0,
                 'geospatial_circle': 0,
                 'generic': 0}}

var n_required_temporal = 0;
var n_required_geospatial_bb = 0;
var n_required_geospatial_circle = 0;
var n_required_generic = 0;

var n_desired_temporal = 0;
var n_desired_geospatial_bb = 0;
var n_desired_geospatial_circle = 0;
var n_desired_generic = 0;

function variableChange(select_id, div_id) {
    var select = document.getElementById(select_id);
    window["add_" + select.options[select.selectedIndex].value](div_id);
}

function add_temporal(div_id) {
    indices[div_id]['temporal'] += 1
    var index = indices[div_id]['temporal'];

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

function add_geospatial_bb(div_id) {
    indices[div_id]['geospatial_bb'] += 1
    var index = indices[div_id]['geospatial_bb'];

    var variables_div = document.getElementById(div_id);

    var _title = document.createElement('h5')
    _title.innerHTML = 'Geospatial Information (Bounding Box)'
    variables_div.appendChild(_title)

    var _div_latitude_1_1 = document.createElement('div')
    _div_latitude_1_1.setAttribute('class', 'form-row')
    var _div_latitude_1_2 = document.createElement('div')
    _div_latitude_1_2.setAttribute('class', 'form-group col-md-8')
    var _label_latitude_1 = document.createElement('label')
    _label_latitude_1.setAttribute('for', 'latitude-1-' + index)
    _label_latitude_1.innerHTML = 'Latitude (Y) Top:'
    _div_latitude_1_2.appendChild(_label_latitude_1)
    var _input_latitude_1 = document.createElement('input')
    _input_latitude_1.setAttribute('class', 'form-control')
    _input_latitude_1.setAttribute('id', 'latitude-1-' + index)
    _div_latitude_1_2.appendChild(_input_latitude_1)
    _div_latitude_1_1.appendChild(_div_latitude_1_2)
    variables_div.appendChild(_div_latitude_1_1)

    var _div_latitude_2_1 = document.createElement('div')
    _div_latitude_2_1.setAttribute('class', 'form-row')
    var _div_latitude_2_2 = document.createElement('div')
    _div_latitude_2_2.setAttribute('class', 'form-group col-md-8')
    var _label_latitude_2 = document.createElement('label')
    _label_latitude_2.setAttribute('for', 'latitude-2-' + index)
    _label_latitude_2.innerHTML = 'Latitude (Y) Bottom:'
    _div_latitude_2_2.appendChild(_label_latitude_2)
    var _input_latitude_2 = document.createElement('input')
    _input_latitude_2.setAttribute('class', 'form-control')
    _input_latitude_2.setAttribute('id', 'latitude-2-' + index)
    _div_latitude_2_2.appendChild(_input_latitude_2)
    _div_latitude_2_1.appendChild(_div_latitude_2_2)
    variables_div.appendChild(_div_latitude_2_1)

    var _div_longitude_1_1 = document.createElement('div')
    _div_longitude_1_1.setAttribute('class', 'form-row')
    var _div_longitude_1_2 = document.createElement('div')
    _div_longitude_1_2.setAttribute('class', 'form-group col-md-8')
    var _label_longitude_1 = document.createElement('label')
    _label_longitude_1.setAttribute('for', 'longitude-1-' + index)
    _label_longitude_1.innerHTML = 'Longitude (X) Left:'
    _div_longitude_1_2.appendChild(_label_longitude_1)
    var _input_longitude_1 = document.createElement('input')
    _input_longitude_1.setAttribute('class', 'form-control')
    _input_longitude_1.setAttribute('id', 'longitude-1-' + index)
    _div_longitude_1_2.appendChild(_input_longitude_1)
    _div_longitude_1_1.appendChild(_div_longitude_1_2)
    variables_div.appendChild(_div_longitude_1_1)

    var _div_longitude_2_1 = document.createElement('div')
    _div_longitude_2_1.setAttribute('class', 'form-row')
    var _div_longitude_2_2 = document.createElement('div')
    _div_longitude_2_2.setAttribute('class', 'form-group col-md-8')
    var _label_longitude_2 = document.createElement('label')
    _label_longitude_2.setAttribute('for', 'longitude-2-' + index)
    _label_longitude_2.innerHTML = 'Longitude (X) Right:'
    _div_longitude_2_2.appendChild(_label_longitude_2)
    var _input_longitude_2 = document.createElement('input')
    _input_longitude_2.setAttribute('class', 'form-control')
    _input_longitude_2.setAttribute('id', 'longitude-2-' + index)
    _div_longitude_2_2.appendChild(_input_longitude_2)
    _div_longitude_2_1.appendChild(_div_longitude_2_2)
    variables_div.appendChild(_div_longitude_2_1)
}

function add_geospatial_circle(div_id) {
    indices[div_id]['geospatial_circle'] += 1
    var index = indices[div_id]['geospatial_circle'];

    var variables_div = document.getElementById(div_id);

    var _title = document.createElement('h5')
    _title.innerHTML = 'Geospatial Information (Circle)'
    variables_div.appendChild(_title)

    var _div_latitude_1 = document.createElement('div')
    _div_latitude_1.setAttribute('class', 'form-row')
    var _div_latitude_2 = document.createElement('div')
    _div_latitude_2.setAttribute('class', 'form-group col-md-8')
    var _label_latitude_1 = document.createElement('label')
    _label_latitude_1.setAttribute('for', 'latitude-' + index)
    _label_latitude_1.innerHTML = 'Latitude (Y) of Center Point:'
    _div_latitude_2.appendChild(_label_latitude_1)
    var _input_latitude_1 = document.createElement('input')
    _input_latitude_1.setAttribute('class', 'form-control')
    _input_latitude_1.setAttribute('id', 'latitude-' + index)
    _div_latitude_2.appendChild(_input_latitude_1)
    _div_latitude_1.appendChild(_div_latitude_2)
    variables_div.appendChild(_div_latitude_1)

    var _div_longitude_1 = document.createElement('div')
    _div_longitude_1.setAttribute('class', 'form-row')
    var _div_longitude_2 = document.createElement('div')
    _div_longitude_2.setAttribute('class', 'form-group col-md-8')
    var _label_longitude_1 = document.createElement('label')
    _label_longitude_1.setAttribute('for', 'longitude-' + index)
    _label_longitude_1.innerHTML = 'Longitude (X) of Center Point:'
    _div_longitude_2.appendChild(_label_longitude_1)
    var _input_longitude_1 = document.createElement('input')
    _input_longitude_1.setAttribute('class', 'form-control')
    _input_longitude_1.setAttribute('id', 'longitude-' + index)
    _div_longitude_2.appendChild(_input_longitude_1)
    _div_longitude_1.appendChild(_div_longitude_2)
    variables_div.appendChild(_div_longitude_1)

    var _div_radius_1 = document.createElement('div')
    _div_radius_1.setAttribute('class', 'form-row')
    var _div_radius_2 = document.createElement('div')
    _div_radius_2.setAttribute('class', 'form-group col-md-8')
    var _label_radius = document.createElement('label')
    _label_radius.setAttribute('for', 'radius-' + index)
    _label_radius.innerHTML = 'Radius:'
    _div_radius_2.appendChild(_label_radius)
    var _input_radius = document.createElement('input')
    _input_radius.setAttribute('class', 'form-control')
    _input_radius.setAttribute('id', 'radius-' + index)
    _div_radius_2.appendChild(_input_radius)
    _div_radius_1.appendChild(_div_radius_2)
    variables_div.appendChild(_div_radius_1)
}

function add_generic(div_id) {
    indices[div_id]['generic'] += 1
    var index = indices[div_id]['generic'];

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
      if(indices[key]['temporal'] > 0) {
        for (i = 1; i <= indices[key]['temporal']; i++) {
            var start = document.getElementById('start-' + i).value;
            var end = document.getElementById('end-' + i).value;
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
      if(indices[key]['geospatial_bb'] > 0) {
        for (i = 1; i <= indices[key]['geospatial_bb']; i++) {
            var latitude1 = document.getElementById('latitude-1-' + i).value;
            var latitude2 = document.getElementById('latitude-2-' + i).value;
            var longitude1 = document.getElementById('longitude-1-' + i).value;
            var longitude2 = document.getElementById('longitude-2-' + i).value;
            if(latitude1 && latitude2 && longitude1 && longitude2) {
                var variable = {'type': 'geospatial_entity'};
                variable['bounding_box'] = {"latitude1": latitude1,
                                            "latitude2": latitude2,
                                            "longitude1": longitude1,
                                            "longitude2": longitude2}
                search.query[key + '_variables'].push(variable)
            }
        }
      }
      if(indices[key]['geospatial_circle'] > 0) {
        for (i = 1; i <= indices[key]['geospatial_circle']; i++) {
            var latitude = document.getElementById('latitude-' + i).value;
            var longitude = document.getElementById('longitude-' + i).value;
            var radius = document.getElementById('radius-' + i).value;
            if(latitude && longitude && radius) {
                var variable = {'type': 'geospatial_entity'};
                variable['circle'] = {"latitude": latitude,
                                            "longitude": longitude,
                                            "radius": radius}
                search.query[key + '_variables'].push(variable)
            }
        }
      }
      if(indices[key]['generic'] > 0) {
        for (i = 1; i <= indices[key]['generic']; i++) {
            var column_names = document.getElementById('name-' + i).value;
            column_names = column_names.split(/[ ,+]+/);
            if(column_names.length == 1 && column_names[0] === '') {
                column_names = [];
            }
            var str_types = [];
            var sem_types = [];
            if(document.getElementById('type-integer-' + i).checked) {
                str_types.push('http://schema.org/Integer');
            }
            if(document.getElementById('type-float-' + i).checked) {
                str_types.push('http://schema.org/Float');
            }
            if(document.getElementById('type-bool-' + i).checked) {
                sem_types.push('http://schema.org/Boolean');
            }
            if(document.getElementById('type-text-' + i).checked) {
                str_types.push('http://schema.org/Text');
            }
            if(document.getElementById('type-datetime-' + i).checked) {
                sem_types.push('http://schema.org/DateTime');
            }
            if(document.getElementById('type-spatial-' + i).checked) {
                sem_types.push('http://schema.org/latitude');
                sem_types.push('http://schema.org/longitude');
            }
            if(document.getElementById('type-phone-' + i).checked) {
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

    // search columns

    var search_columns = document.getElementById('search_columns').value;
    search_columns = search_columns.split(/[ ,+]+/);
    if(search_columns.length == 1 && search_columns[0] === '') {
        search_columns = [];
    } else {
        search.query['required_variables'].push(
            {'type': 'dataframe_columns',
             'names': search_columns}
        );
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
