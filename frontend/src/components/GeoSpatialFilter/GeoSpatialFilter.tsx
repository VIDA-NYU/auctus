import React from 'react';
import * as ol from 'ol';
import { toStringHDMS } from 'ol/coordinate';
import { Draw } from 'ol/interaction';
import { createBox } from 'ol/interaction/Draw';
import GeometryType from 'ol/geom/GeometryType';
import {
  Tile as TileLayer,
  Vector as VectorLayer
} from 'ol/layer'
import {
  Vector as VectorSource,
  OSM as OSMSource,
} from 'ol/source'
import {
  ScaleLine,
  ZoomSlider,
  MousePosition,
  OverviewMap,
  defaults as DefaultControls
} from 'ol/control'

import {
  fromLonLat,
} from 'ol/proj'

class GeoSpatialFilter extends React.Component {

  mapId = Math.random().toString(36).substr(2, 9);

  state = {
    selectedCoordinates: undefined,
  };

  map = undefined;
  mapSource = undefined;
  draw = undefined;

  componentDidMount() {

    const openStreetMapTileLayer = new TileLayer({
      source: new OSMSource()
    });

    const source = new VectorSource({ wrapX: false });
    const vectorLayer = new VectorLayer({ source });

    // @ts-ignore
    source.on('addfeature', (evt) => this.onSelectCoordinates(evt));

    this.mapSource = source;
    this.map = new ol.Map({
      target: this.mapId,
      layers: [openStreetMapTileLayer, vectorLayer],
      // Add in the following map controls
      // @ts-ignore
      controls: DefaultControls().extend([
        new ZoomSlider(),
        new MousePosition(),
        new ScaleLine(),
        new OverviewMap()
      ]),
      view: new ol.View({
        projection: 'EPSG:3857',
        center: fromLonLat([-73.986579, 40.6942036], 'EPSG:3857'), // Tandon
        zoom: 12
        // center: fromLonLat([0, 0], 'EPSG:3857'),
        // zoom: 2
      })
    });

    this.map.getViewport().addEventListener('contextmenu', (evt) => {
      // the 'contextmenu' event is triggered on right-button click
      // we use it to clear the current coordinates selection
      this.mapSource.clear();
      this.setState({ selectedCoordinates: undefined });
    })

    this.addInteractions();
  }

  onSelectCoordinates(evt) {
    const geometry = evt.feature.getGeometry();

    const coord = geometry.clone().transform('EPSG:3857', 'EPSG:4326').getCoordinates()[0];
    const topLeftLat = coord[3][1];
    const topLeftLon = coord[3][0];
    const bottomRightLat = coord[1][1];
    const bottomRightLon = coord[1][0];

    const topLeftText = toStringHDMS([topLeftLon, topLeftLat]);
    const topRightText = toStringHDMS([bottomRightLon, bottomRightLat]);

    this.setState({ selectedCoordinates: { topLeftText, topRightText } });
  }

  addInteractions() {
    this.mapSource.clear();
    const draw = new Draw({
      source: this.mapSource,
      type: GeometryType.CIRCLE,
      geometryFunction: createBox(),
      condition: (e) => {
        // when the point's button is 1 (leftclick), allows drawing
        if (e.pointerEvent.buttons === 1) {
          return true;
        } else {
          return false;
        }
      }
    });

    // @ts-ignore
    draw.on('drawstart', (e) => this.mapSource.clear());

    if (this.draw) {
      this.map.removeInteraction(this.draw);
    }
    this.draw = draw;
    this.map.addInteraction(this.draw);
  }

  render() {
    const style = {
      width: '100%',
      height: '400px',
    }
    return (
      <div>
        <div id={this.mapId} style={style} />
        <div>
          {
            this.state.selectedCoordinates &&
            <>
              <span>Top Left: <code>{this.state.selectedCoordinates.topLeftText}</code></span>
              <span className="ml-3">Bottom Right: <code>{this.state.selectedCoordinates.topRightText}</code></span>
            </>
          }
        </div>
      </div>
    )
  }
}

export { GeoSpatialFilter };

