import React from 'react';
import { generateRandomId } from '../../utils';
import { Map, View, Feature } from 'ol/';
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
import { GeoSpatialVariable } from '../../api/types';
import { PersistentComponent } from '../visus/PersistentComponent/PersistentComponent';
import {
  transformCoordinates,
  centralizeMapToFeature,
  MyMapBrowserEvent,
} from '../spatial-utils';
import 'ol/ol.css';

interface GeoSpatialFilterState {
  selectedCoordinates?: {
    topLeftText: string;
    topRightText: string;
  };
  feature?: Feature;
}

interface GeoSpatialFilterProps {
  onSelectCoordinates: (coordinates: GeoSpatialVariable) => void;
}

class GeoSpatialFilter extends PersistentComponent<
  GeoSpatialFilterProps,
  GeoSpatialFilterState
> {
  mapId = generateRandomId();
  map?: Map;
  source?: VectorSource;

  constructor(props: GeoSpatialFilterProps) {
    super(props);
    this.state = {
      selectedCoordinates: undefined,
    };
  }

  componentDidUpdate() {
    // The OpenLayers map looses its selected state when the react component
    // is unmounted. Here we re-load the selected feature from the previous
    // component state.
    if (!this.map) {
      return;
    }
    if (
      this.source &&
      this.source.getFeatures().length === 0 &&
      this.state.feature
    ) {
      this.source.addFeature(this.state.feature);
      centralizeMapToFeature(this.map, this.state.feature);
    }
  }

  componentDidMount() {
    super.componentDidMount();

    const openStreetMapTileLayer = new TileLayer({
      source: new OSMSource(),
    });

    this.source = new VectorSource({ wrapX: false });
    this.source.on('addfeature', evt => this.onSelectCoordinates(evt));

    const vectorLayer = new VectorLayer({ source: this.source });
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
        // center: fromLonLat([-73.986579, 40.6942036], 'EPSG:3857'), // Tandon
        // zoom: 12,
        center: fromLonLat([0, 0], 'EPSG:3857'),
        zoom: 2,
      }),
    });

    map.getViewport().addEventListener('contextmenu', () => {
      // the 'contextmenu' event is triggered on right-button click
      // we use it to clear the current coordinates selection
      if (this.source) {
        this.source.clear();
      }
      this.setState({ selectedCoordinates: undefined });
    });

    this.addInteractions(map, this.source);
    this.map = map;
  }

  onSelectCoordinates(evt: VectorSourceEvent) {
    const {
      topLeftLat,
      topLeftLon,
      bottomRightLat,
      bottomRightLon,
    } = transformCoordinates(evt.feature);

    const topLeftText = toStringHDMS([topLeftLon, topLeftLat]);
    const topRightText = toStringHDMS([bottomRightLon, bottomRightLat]);

    this.setState({
      selectedCoordinates: { topLeftText, topRightText },
      feature: evt.feature,
    });
    this.props.onSelectCoordinates({
      type: 'geospatial_variable',
      latitude1: topLeftLat.toString(),
      longitude1: topLeftLon.toString(),
      latitude2: bottomRightLat.toString(),
      longitude2: bottomRightLon.toString(),
    });
  }

  addInteractions(map: Map, mapSource: VectorSource) {
    mapSource.clear();
    const draw = new Draw({
      source: mapSource,
      type: GeometryType.CIRCLE,
      geometryFunction: createBox(),
      condition: (evt: MapBrowserEvent) => {
        const e = evt as MyMapBrowserEvent; // workaround for type definition bug
        // when the point's button is 1 (left-click), allows drawing
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
        <div className="row">
          <div className="col-md-12" style={{ fontSize: '.9rem' }}>
            <span className="d-inline">
              Left-click to start selection. Right-click to clear selection.
            </span>
            <div className="d-inline" style={{ float: 'right' }}>
              {this.state.selectedCoordinates && (
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
        </div>
        <div id={this.mapId} style={style} className="map" />
      </div>
    );
  }
}

export { GeoSpatialFilter };
