import React from 'react';
import { Map, View } from 'ol/';
import { toStringHDMS } from 'ol/coordinate';
import { Draw } from 'ol/interaction';
import { createBox } from 'ol/interaction/Draw';
import GeometryType from 'ol/geom/GeometryType';
import { Tile as TileLayer, Vector as VectorLayer } from 'ol/layer';
import { Vector as VectorSource, OSM as OSMSource } from 'ol/source';
import {
  ScaleLine,
  ZoomSlider,
  MousePosition,
  OverviewMap,
  defaults as DefaultControls,
} from 'ol/control';
import { fromLonLat } from 'ol/proj';
import { VectorSourceEvent } from 'ol/source/Vector';
import MapBrowserEvent from 'ol/MapBrowserEvent';
import Geometry from 'ol/geom/Geometry';

//
// Following types are a temporary workaround to a bug in typings from the
// OpenLayers library (package @types/ol) This may be removed after upgrading
// this library to a newer version
//
interface MyMapBrowserEvent extends MapBrowserEvent {
  pointerEvent: PointerEvent;
}

interface MyGeometry extends Geometry {
  getCoordinates(): number[][][];
}

interface GeoSpatialFilterState {
  selectedCoordinates?: {
    topLeftText: string;
    topRightText: string;
  };
}

class GeoSpatialFilter extends React.Component<{}, GeoSpatialFilterState> {
  mapId = Math.random()
    .toString(36)
    .substr(2, 9);

  constructor(props: {}) {
    super(props);
    this.state = {
      selectedCoordinates: undefined,
    };
  }

  componentDidMount() {
    const openStreetMapTileLayer = new TileLayer({
      source: new OSMSource(),
    });

    const source = new VectorSource({ wrapX: false });
    const vectorLayer = new VectorLayer({ source });

    source.on('addfeature', evt => this.onSelectCoordinates(evt));

    const map = new Map({
      target: this.mapId,
      layers: [openStreetMapTileLayer, vectorLayer],
      // Add in the following map controls
      controls: DefaultControls().extend([
        new ZoomSlider(),
        new MousePosition(),
        new ScaleLine(),
        new OverviewMap(),
      ]),
      view: new View({
        projection: 'EPSG:3857',
        center: fromLonLat([-73.986579, 40.6942036], 'EPSG:3857'), // Tandon
        zoom: 12,
        // center: fromLonLat([0, 0], 'EPSG:3857'),
        // zoom: 2
      }),
    });

    map.getViewport().addEventListener('contextmenu', () => {
      // the 'contextmenu' event is triggered on right-button click
      // we use it to clear the current coordinates selection
      if (source) {
        source.clear();
      }
      this.setState({ selectedCoordinates: undefined });
    });

    this.addInteractions(map, source);
  }

  onSelectCoordinates(evt: VectorSourceEvent) {
    const geometry = evt.feature.getGeometry();
    if (!geometry) {
      return;
    }
    const transformedGeometry = geometry
      .clone()
      .transform('EPSG:3857', 'EPSG:4326') as MyGeometry;
    const coord = transformedGeometry.getCoordinates()[0];
    const topLeftLat = coord[3][1];
    const topLeftLon = coord[3][0];
    const bottomRightLat = coord[1][1];
    const bottomRightLon = coord[1][0];

    const topLeftText = toStringHDMS([topLeftLon, topLeftLat]);
    const topRightText = toStringHDMS([bottomRightLon, bottomRightLat]);

    this.setState({ selectedCoordinates: { topLeftText, topRightText } });
  }

  addInteractions(map: Map, mapSource: VectorSource) {
    mapSource.clear();
    const draw = new Draw({
      source: mapSource,
      type: GeometryType.CIRCLE,
      geometryFunction: createBox(),
      condition: (evt: MapBrowserEvent) => {
        const e = evt as MyMapBrowserEvent; // workaround for type definition bug
        // when the point's button is 1 (leftclick), allows drawing
        if (e.pointerEvent.buttons === 1) {
          return true;
        } else {
          return false;
        }
      },
    });

    draw.on('drawstart', (e: DragEvent) => mapSource.clear());
    map.addInteraction(draw);
  }

  render() {
    const style = {
      width: '100%',
      height: '400px',
    };
    return (
      <div>
        <div id={this.mapId} style={style} />
        <div>
          {this.state && this.state.selectedCoordinates && (
            <>
              <span>
                Top Left:{' '}
                <code>{this.state.selectedCoordinates.topLeftText}</code>
              </span>
              <span className="ml-3">
                Bottom Right:{' '}
                <code>{this.state.selectedCoordinates.topRightText}</code>
              </span>
            </>
          )}
        </div>
      </div>
    );
  }
}

export { GeoSpatialFilter };
