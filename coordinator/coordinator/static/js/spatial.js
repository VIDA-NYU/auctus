function show_spatial_coverage(coverage) {
  var maps = [];
  var polygons = [];
  var sources = [];
  var overlays = [];
  var extents = [];
  var index = 0;

  if(coverage.length > 0) {
    var div = document.getElementById('spatial_coverage');

    for(var i = 0; i < coverage.length; i++) {
      var element = coverage[i];

      if(element['ranges'].length == 0) {
        continue;
      }

      var _title = document.createElement('h5');
      _title.innerHTML = ('Lat. Variable: ' +
                          element['lat'] +
                          ' &nbsp; &nbsp;| &nbsp; &nbsp; Lon. Variable: ' +
                          element['lon']);
      div.appendChild(_title);

      var map_div = document.createElement('div');
      map_div.setAttribute('id', 'map-' + index);
      map_div.setAttribute('class', 'map mb-1');
      div.appendChild(map_div);

      var info = document.createElement('p');
      info.setAttribute('class', 'mb-3');
      info.setAttribute('style', 'font-size: 11px;');
      info.innerHTML = 'Left-click on bounding box to get more information.';
      div.appendChild(info);

      var popup_div = document.createElement('div');
      popup_div.setAttribute('id', 'popup-' + index);
      popup_div.setAttribute('class', 'ol-popup');
      var popup_content = document.createElement('div');
      popup_content.setAttribute('id', 'popup-content-' + index);
      popup_div.appendChild(popup_content);
      div.appendChild(popup_div);

      // finding outer bounding box
      // collecting all the bounding boxes
      polygons.push([]);
      var top_left = element['ranges'][0]['range']['coordinates'][0];
      var bottom_right = element['ranges'][0]['range']['coordinates'][1];
      var min_x = top_left[0];
      var max_x = bottom_right[0];
      var min_y = bottom_right[1];
      var max_y = top_left[1];
      polygons[index].push([
        [top_left[0], top_left[1]],
        [top_left[0], bottom_right[1]],
        [bottom_right[0], bottom_right[1]],
        [bottom_right[0], top_left[1]],
        [top_left[0], top_left[1]]
      ]);
      for(var j = 1; j < element['ranges'].length; j++) {
        top_left = element['ranges'][j]['range']['coordinates'][0];
        bottom_right = element['ranges'][j]['range']['coordinates'][1];
        min_x = Math.min(top_left[0], min_x);
        max_x = Math.max(bottom_right[0], max_x);
        min_y = Math.min(bottom_right[1], min_y);
        max_y = Math.max(top_left[1], max_y);

        polygons[index].push([
          [top_left[0], top_left[1]],
          [top_left[0], bottom_right[1]],
          [bottom_right[0], bottom_right[1]],
          [bottom_right[0], top_left[1]],
          [top_left[0], top_left[1]]
        ]);
      }

      // Creating Map

      var raster = new ol.layer.Tile({
        source: new ol.source.OSM()
      });

      var source = new ol.source.Vector({wrapX: false});

      // drawing bounding boxes
      for(var j = 0; j < polygons[index].length; j++) {
        var polygon = new ol.geom.Polygon([polygons[index][j]]);
        polygon.transform('EPSG:4326', 'EPSG:3857');
        var feature = new ol.Feature(polygon);
        source.addFeature(feature);
      }

      sources.push(source);

      var style = new ol.style.Style({
        stroke: new ol.style.Stroke({
          'color': '#57068c',
          'width': 3
        }),
        fill: new ol.style.Fill({
          'color': '#ffffff'
        })
      });

      var vector = new ol.layer.Vector({
        source: sources[index],
        style: style,
        opacity: 0.5
      });

      // popup with bounding boxes
      var container = document.getElementById('popup-' + index);

      var overlay = new ol.Overlay({
        id: 'overlay',
        element: container,
        autoPan: true,
        autoPanAnimation: {
          duration: 250
        }
      });
      overlays.push(overlay);

      var map = new ol.Map({
        layers: [raster, vector],
        overlays: [overlays[index]],
        target: 'map-' + index,
        view: new ol.View({
          projection: 'EPSG:3857',
          center: [0, 0],
          zoom: 0
        })
      });
      maps.push(map);

      // Centralize map

      var extent = ol.proj.transformExtent(
        [min_x, min_y, max_x, max_y],
        "EPSG:4326", "EPSG:3857"
      );
      extents.push(extent);

      index += 1;
    }

    div.setAttribute('style', 'display: block;');

    // hover over bounding boxes to show lat / lon
    for(var i = 0; i < maps.length; i++) {

      var selectClick = new ol.interaction.Select({
        condition: ol.events.condition.click
      });

      maps[i].addInteraction(selectClick);

      selectClick.on('select', function(evt) {
        var feature = this.getMap().forEachFeatureAtPixel(evt.mapBrowserEvent.pixel, function(feature) {
          return feature;
        });
        if(feature) {
          var geometry = feature.getGeometry();
          var coord = geometry.clone().transform('EPSG:3857', 'EPSG:4326').getCoordinates()[0];
          var top_left_lat = coord[0][1];
          var top_left_lon = coord[0][0];
          var bottom_right_lat = coord[2][1];
          var bottom_right_lon = coord[2][0];

          var top_left = ol.coordinate.toStringHDMS([top_left_lon, top_left_lat]);
          var bottom_right = ol.coordinate.toStringHDMS([bottom_right_lon, bottom_right_lat]);

          var content = document.getElementById('popup-content-' + this.getMap().getTarget().split("-")[1]);
          content.innerHTML = ('<p style="margin-bottom: 0px;">Top Left:</p><code>' + top_left + '</code>' +
                               '<p style="margin-bottom: 0px;">Bottom Right:</p><code>' + bottom_right + '</code>');
          this.getMap().getOverlayById('overlay').setPosition(
            ol.proj.transform(
              [top_left_lon, top_left_lat],
              'EPSG:4326', 'EPSG:3857')
            );
          } else {
            this.getMap().getOverlayById('overlay').setPosition(undefined);
          }
      });

    }

    // Centralize map
    for(var i = 0; i < maps.length; i++) {
      maps[i].getView().fit(extents[i]);
      maps[i].updateSize();
    }
  }
}
