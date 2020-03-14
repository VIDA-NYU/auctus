import React from 'react';
import { generateRandomId } from '../../utils';
import { Map, View, Feature, Overlay } from 'ol/';
import { toStringHDMS } from 'ol/coordinate';
import { Select } from 'ol/interaction';
import { Tile as TileLayer, Vector as VectorLayer } from 'ol/layer';
import { Vector as VectorSource, OSM as OSMSource } from 'ol/source';
import { transformExtent, transform } from 'ol/proj';
import { SpatialCoverage } from '../../api/types';
import PersistentComponent from '../visus/PersistentComponent/PersistentComponent';
import Polygon from 'ol/geom/Polygon';
import Style from 'ol/style/Style';
import Stroke from 'ol/style/Stroke';
import Fill from 'ol/style/Fill';
import { click } from 'ol/events/condition';
import './GeoSpatialCoverageMap.css';
import { transformCoordinates } from '../spatial-utils';

interface GeoSpatialCoverageMapProps {
  coverage: SpatialCoverage;
}

class GeoSpatialCoverageMap extends PersistentComponent<
  GeoSpatialCoverageMapProps
> {
  mapId = generateRandomId();
  mapRef: React.RefObject<HTMLDivElement>;
  containerRef: React.RefObject<HTMLDivElement>;
  popupContentRef: React.RefObject<HTMLDivElement>;

  constructor(props: GeoSpatialCoverageMapProps) {
    super(props);
    this.containerRef = React.createRef();
    this.popupContentRef = React.createRef();
    this.mapRef = React.createRef();
    this.state = {
      selectedCoordinates: undefined,
    };
  }

  createPolygons(element: SpatialCoverage) {
    // collect all the bounding boxes and find their
    // extent (outer bounding box)
    const polygons = [];

    let top_left = element.ranges[0].range.coordinates[0];
    let bottom_right = element.ranges[0].range.coordinates[1];
    let min_x = top_left[0];
    let max_x = bottom_right[0];
    let min_y = bottom_right[1];
    let max_y = top_left[1];

    polygons.push([
      [top_left[0], top_left[1]],
      [top_left[0], bottom_right[1]],
      [bottom_right[0], bottom_right[1]],
      [bottom_right[0], top_left[1]],
      [top_left[0], top_left[1]],
    ]);

    for (var j = 1; j < element.ranges.length; j++) {
      top_left = element.ranges[j].range.coordinates[0];
      bottom_right = element.ranges[j].range.coordinates[1];
      min_x = Math.min(top_left[0], min_x);
      max_x = Math.max(bottom_right[0], max_x);
      min_y = Math.min(bottom_right[1], min_y);
      max_y = Math.max(top_left[1], max_y);

      polygons.push([
        [top_left[0], top_left[1]],
        [top_left[0], bottom_right[1]],
        [bottom_right[0], bottom_right[1]],
        [bottom_right[0], top_left[1]],
        [top_left[0], top_left[1]],
      ]);
    }

    return {
      extent: [min_x, min_y, max_x, max_y],
      polygons,
    };
  }

  componentDidMount() {
    this.setupMap();
  }

  componentDidUpdate() {
    // Remove all children from the map div to force re-render
    const mapNode = this.mapRef.current;
    if (mapNode) {
      while (mapNode.firstChild) {
        if (mapNode.lastChild) {
          mapNode.removeChild(mapNode.lastChild);
        }
      }
      this.setupMap();
    }
  }

  setupMap() {
    const { polygons, extent } = this.createPolygons(this.props.coverage);

    const raster = new TileLayer({
      source: new OSMSource(),
    });

    const source = new VectorSource({ wrapX: false });

    // drawing bounding boxes
    for (let j = 0; j < polygons.length; j++) {
      const polygon = new Polygon([polygons[j]]);
      polygon.transform('EPSG:4326', 'EPSG:3857');
      const feature = new Feature(polygon);
      source.addFeature(feature);
    }

    const style = new Style({
      stroke: new Stroke({
        color: '#57068c',
        width: 3,
      }),
      fill: new Fill({
        color: '#ffffff',
      }),
    });

    const vector = new VectorLayer({
      source: source,
      style: style,
      opacity: 0.5,
    });

    // popup with bounding boxes
    const container = this.containerRef.current
      ? this.containerRef.current
      : undefined;

    const overlay = new Overlay({
      id: 'overlay',
      element: container,
      autoPan: true,
      autoPanAnimation: {
        duration: 250,
      },
    });

    const map = new Map({
      layers: [raster, vector],
      overlays: [overlay], // [overlays[index]],
      target: this.mapId, //'map-' + index,
      view: new View({
        projection: 'EPSG:3857',
        center: [0, 0],
        zoom: 0,
      }),
    });

    // Centralize map
    const olExtent = transformExtent(extent, 'EPSG:4326', 'EPSG:3857');
    map.getView().fit(olExtent);
    map.updateSize();

    this.setupHoverPopUp(map);
  }

  setupHoverPopUp(map: Map) {
    var selectClick = new Select({
      condition: click,
    });

    map.addInteraction(selectClick);
    const that = this;
    selectClick.on('select', function(evt) {
      var feature = map.forEachFeatureAtPixel(
        evt.mapBrowserEvent.pixel,
        function(feature) {
          return feature;
        }
      );
      if (feature) {
        const {
          topLeftLat,
          topLeftLon,
          bottomRightLat,
          bottomRightLon,
        } = transformCoordinates(feature);

        var top_left = toStringHDMS([topLeftLon, topLeftLat]);
        var bottom_right = toStringHDMS([bottomRightLon, bottomRightLat]);

        const content = that.popupContentRef.current;
        if (content) {
          content.innerHTML =
            '<span>Top Left: </span><code>' +
            top_left +
            '</code> </br>' +
            '<span>Bottom Right: </span><code>' +
            bottom_right +
            '</code>';
          map
            .getOverlayById('overlay')
            .setPosition(
              transform([topLeftLon, topLeftLat], 'EPSG:4326', 'EPSG:3857')
            );
        }
      } else {
        map.getOverlayById('overlay'); //.setPosition(undefined);
      }
    });
  }

  render() {
    const style = { width: '100%', height: '400px' };
    const { lat, lon } = this.props.coverage;
    return (
      <div style={{ display: 'block' }}>
        <div className="mb-2 mt-2">
          <b>Latitude Column: </b>
          <span className="badge badge-pill badge-secondary mr-1">{lat}</span>
          &nbsp;|&nbsp;&nbsp;
          <b>Longitude Column: </b>
          <span className="badge badge-pill badge-secondary mr-1">{lon}</span>
        </div>
        <div id={this.mapId} ref={this.mapRef} className="map" style={style} />
        <span className="mb-3" style={{ fontSize: '0.9rem' }}>
          Left-click on bounding box to get more information.
        </span>
        <div ref={this.containerRef} className="ol-popup">
          <div ref={this.popupContentRef} />
        </div>
      </div>
    );
  }
}

export { GeoSpatialCoverageMap };
