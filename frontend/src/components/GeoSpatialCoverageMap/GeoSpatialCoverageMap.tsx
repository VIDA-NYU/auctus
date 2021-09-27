import React from 'react';
import {generateRandomId} from '../../utils';
import {Map, View, Feature, Overlay} from 'ol/';
import {toStringHDMS} from 'ol/coordinate';
import {defaults as interactionDefaults} from 'ol/interaction';
import {Select} from 'ol/interaction';
import {Tile as TileLayer, Vector as VectorLayer} from 'ol/layer';
import {Vector as VectorSource, OSM as OSMSource} from 'ol/source';
import {transformExtent, transform} from 'ol/proj';
import {SpatialCoverage} from '../../api/types';
import Polygon from 'ol/geom/Polygon';
import Style from 'ol/style/Style';
import Stroke from 'ol/style/Stroke';
import Fill from 'ol/style/Fill';
import {click} from 'ol/events/condition';
import './GeoSpatialCoverageMap.css';
import {transformCoordinates, centralizeMapToExtent} from '../../spatial-utils';
import 'ol/ol.css';
import {scaleSequential} from 'd3-scale';

// Amount of outlying data to ignore when focusing the map
// 5% on each side
const OUTLIER_RATIO = 0.05;
const leftColorLegend = 'rgb(143, 140, 193)';
const rightColorLegend = 'rgb(65, 2, 136)';

function geohashToLatLong(hash: string, base: number) {
  if (base !== 4) {
    throw new Error('Only geohash4 is implemented');
  }
  const topLeft = [-180, -90];
  const bottomRight = [180, 90];
  for (let i = 0; i < hash.length; i++) {
    {
      const mid = (topLeft[0] + bottomRight[0]) / 2;
      if (hash[i] === '2' || hash[i] === '3') {
        topLeft[0] = mid;
      } else {
        bottomRight[0] = mid;
      }
    }
    {
      const mid = (topLeft[1] + bottomRight[1]) / 2;
      if (hash[i] === '1' || hash[i] === '3') {
        topLeft[1] = mid;
      } else {
        bottomRight[1] = mid;
      }
    }
  }
  return {topLeft, bottomRight};
}

function heatColorMap(
  hashNumber: number,
  maxNumber: number,
  totalNumber: number
): string {
  const value = hashNumber / totalNumber;
  const interpolateCustomPurples = scaleSequential([
    leftColorLegend,
    rightColorLegend,
  ]);
  const color = scaleSequential(interpolateCustomPurples).domain([
    0,
    maxNumber / totalNumber,
  ]);
  return color(value);
}

interface GeoSpatialCoverageMapProps {
  coverage: SpatialCoverage;
  sampled: boolean;
}
interface GeoSpatialCoverageMapState {
  selectedCoordinates: undefined;
  rightValueLegend: number;
}

class GeoSpatialCoverageMap extends React.PureComponent<
  GeoSpatialCoverageMapProps,
  GeoSpatialCoverageMapState
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
      rightValueLegend: 0,
    };
  }

  createCoverage(coverage: SpatialCoverage) {
    let minX = Infinity;
    let maxX = -Infinity;
    let minY = Infinity;
    let maxY = -Infinity;

    const vectorLayers = [];

    if (coverage.geohashes4?.length) {
      // First pass to compute color scale
      let maxNumber = 1;
      let totalNumber = 0;
      for (let j = 0; j < coverage.geohashes4.length; j++) {
        // maxNumber is the number of points in the box with the most points
        maxNumber = Math.max(maxNumber, coverage.geohashes4[j].number);
        totalNumber += coverage.geohashes4[j].number;
      }
      if (coverage.number) {
        totalNumber = coverage.number;
      }

      const minXList = [];
      const maxXList = [];
      const minYList = [];
      const maxYList = [];

      // drawing geohashes
      const source = new VectorSource({wrapX: false});
      for (let j = 0; j < coverage.geohashes4.length; j++) {
        // hashNumber is the number of points in a given box
        const {hash, number: hashNumber} = coverage.geohashes4[j];
        const {topLeft, bottomRight} = geohashToLatLong(hash, 4);
        minXList.push(topLeft[0]);
        maxXList.push(bottomRight[0]);
        minYList.push(bottomRight[1]);
        maxYList.push(topLeft[1]);

        const polygon = new Polygon([
          [
            [topLeft[0], topLeft[1]],
            [topLeft[0], bottomRight[1]],
            [bottomRight[0], bottomRight[1]],
            [bottomRight[0], topLeft[1]],
            [topLeft[0], topLeft[1]],
          ],
        ]);
        const style = new Style({
          fill: new Fill({
            color: heatColorMap(hashNumber, maxNumber, totalNumber),
          }),
        });
        polygon.transform('EPSG:4326', 'EPSG:3857');
        const feature = new Feature(polygon);
        feature.setStyle(style);
        feature.set('numberOfPoints', hashNumber);
        feature.set('ratioOfPoints', hashNumber / totalNumber);
        feature.set('geohash4', hash);
        source.addFeature(feature);
      }
      this.setState({rightValueLegend: 100 * (maxNumber / totalNumber)});
      minXList.sort();
      minX = minXList[Math.floor((minXList.length - 1) * OUTLIER_RATIO)];
      maxXList.sort();
      maxX = maxXList[Math.ceil((maxXList.length - 1) * (1.0 - OUTLIER_RATIO))];
      minYList.sort();
      minY = minYList[Math.floor((minYList.length - 1) * OUTLIER_RATIO)];
      maxYList.sort();
      maxY = maxYList[Math.ceil((maxYList.length - 1) * (1.0 - OUTLIER_RATIO))];

      vectorLayers.push(
        new VectorLayer({
          source,
          opacity: 0.5,
        })
      );
    } else if (coverage.ranges?.length) {
      // drawing bounding boxes
      const source = new VectorSource({wrapX: false});
      for (let j = 0; j < coverage.ranges.length; j++) {
        const topLeft = coverage.ranges[j].range.coordinates[0];
        const bottomRight = coverage.ranges[j].range.coordinates[1];
        minX = Math.min(topLeft[0], minX);
        maxX = Math.max(bottomRight[0], maxX);
        minY = Math.min(bottomRight[1], minY);
        maxY = Math.max(topLeft[1], maxY);

        const polygon = new Polygon([
          [
            [topLeft[0], topLeft[1]],
            [topLeft[0], bottomRight[1]],
            [bottomRight[0], bottomRight[1]],
            [bottomRight[0], topLeft[1]],
            [topLeft[0], topLeft[1]],
          ],
        ]);
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

      vectorLayers.push(
        new VectorLayer({
          source,
          style,
          opacity: 0.8,
        })
      );
    }

    const extent = [minX, minY, maxX, maxY];
    return {extent, vectorLayers};
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
    const {extent, vectorLayers} = this.createCoverage(this.props.coverage);

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

    const rasterLayer = new TileLayer({
      source: new OSMSource(),
    });

    const map = new Map({
      interactions: interactionDefaults({mouseWheelZoom: false}),
      layers: [rasterLayer, ...vectorLayers],
      overlays: [overlay],
      target: this.mapId, //'map-' + index,
      view: new View({
        projection: 'EPSG:3857',
        center: [0, 0],
        zoom: 0,
      }),
    });

    centralizeMapToExtent(
      map,
      transformExtent(extent, 'EPSG:4326', 'EPSG:3857')
    );

    this.setupHoverPopUp(map);
  }

  setupHoverPopUp(map: Map) {
    const selectClick = new Select({
      condition: click,
    });

    map.addInteraction(selectClick);

    selectClick.on('select', evt => {
      const feature = map.forEachFeatureAtPixel(
        evt.mapBrowserEvent.pixel,
        feature => {
          return feature;
        }
      );
      if (feature) {
        const {topLeftLat, topLeftLon, bottomRightLat, bottomRightLon} =
          transformCoordinates(feature);

        const topLeft = toStringHDMS([topLeftLon, topLeftLat]);
        const bottomRight = toStringHDMS([bottomRightLon, bottomRightLat]);

        const content = this.popupContentRef.current;
        if (content) {
          const isGeohash = feature.get('geohash4') !== undefined;
          const numberOfPoints = feature.get('numberOfPoints');
          if (isGeohash && numberOfPoints !== undefined) {
            const percent = (100 * feature.get('ratioOfPoints')).toFixed(2);
            if (this.props.sampled) {
              content.innerHTML = `${percent} % of points`;
            } else {
              content.innerHTML = `${numberOfPoints} points, ${percent} %`;
            }
          } else if (!isGeohash) {
            content.innerHTML =
              '<span>Top Left: </span><code>' +
              topLeft +
              '</code> </br>' +
              '<span>Bottom Right: </span><code>' +
              bottomRight +
              '</code>';
          } else {
            return;
          }
          map
            .getOverlayById('overlay')
            .setPosition(
              transform(
                [
                  (topLeftLon + bottomRightLon) / 2,
                  (topLeftLat + bottomRightLat) / 2,
                ],
                'EPSG:4326',
                'EPSG:3857'
              )
            );
        }
      } else {
        map.getOverlayById('overlay').setPosition(undefined);
      }
    });
  }

  renderCoverageColumns(coverage: SpatialCoverage) {
    // Keep in sync, search code for 279a32
    const {column_names, type} = coverage;
    if (type === 'latlong' && column_names.length === 2) {
      return (
        <>
          <b>Latitude Column: </b>
          <span className="badge badge-pill badge-secondary mr-1">
            {column_names[0]}
          </span>
          &nbsp;|&nbsp;&nbsp;
          <b>Longitude Column: </b>
          <span className="badge badge-pill badge-secondary mr-1">
            {column_names[1]}
          </span>
        </>
      );
    } else if (type === 'address' && column_names.length === 1) {
      return (
        <>
          <b>Address Column: </b>
          <span className="badge badge-pill badge-secondary mr-1">
            {column_names[0]}
          </span>
        </>
      );
    } else if (type === 'point' && column_names.length === 1) {
      return (
        <>
          <b>Point Column (long-lat): </b>
          <span className="badge badge-pill badge-secondary mr-1">
            {column_names[0]}
          </span>
        </>
      );
    } else if (type === 'point_latlong' && column_names.length === 1) {
      return (
        <>
          <b>Point Column (lat-long): </b>
          <span className="badge badge-pill badge-secondary mr-1">
            {column_names[0]}
          </span>
        </>
      );
    } else if (type === 'admin' && column_names.length === 1) {
      return (
        <>
          <b>Administrative Area Column: </b>
          <span className="badge badge-pill badge-secondary mr-1">
            {column_names[0]}
          </span>
        </>
      );
    } else {
      return (
        <>
          <b>Other Coverage: </b>
          {column_names.map((n, i) => (
            <span key={i} className="badge badge-pill badge-secondary mr-1">
              {n}
            </span>
          ))}
        </>
      );
    }
  }

  renderLegend() {
    const marginLegend = 12;
    const legendWidth = 130 + marginLegend;
    return (
      <div className="legend" style={{width: legendWidth}}>
        <svg height="50" width={legendWidth} fill="black">
          <g
            transform="translate(0,28)"
            fill="none"
            fontSize="10"
            fontFamily="sans-serif"
            textAnchor="middle"
          >
            <g
              className="tick"
              opacity="1"
              transform={'translate(' + marginLegend + ',10)'}
            >
              <line stroke="currentColor" x="0" y2="1" y1="-4"></line>
              <text
                fill="black"
                style={{fontSize: 9, fontFamily: 'sans-serif'}}
                x="0"
                y="3"
                dy="0.71em"
              >
                0
              </text>
            </g>
            <g
              className="tick"
              opacity="1"
              transform={
                'translate(' + (legendWidth - marginLegend) / 2 + ',10)'
              }
            >
              <line stroke="currentColor" x="0" y2="1" y1="-4"></line>
              <text
                fill="black"
                style={{fontSize: 9, fontFamily: 'sans-serif'}}
                x="0"
                y="3"
                dy="0.71em"
              >
                {(this.state.rightValueLegend / 2).toFixed(1)}
              </text>
            </g>

            <g
              className="tick"
              opacity="1"
              transform={'translate(' + (legendWidth - marginLegend) + ',10)'}
            >
              <line stroke="currentColor" x="0" y2="1" y1="-4"></line>
              <text
                fill="black"
                style={{fontSize: 9, fontFamily: 'sans-serif'}}
                x="0"
                y="3"
                dy="0.71em"
              >
                {this.state.rightValueLegend.toFixed(1)}
              </text>
            </g>
          </g>
          <defs>
            <linearGradient id="grad1" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop
                offset="0%"
                style={{stopColor: leftColorLegend, stopOpacity: 0.7}}
              />
              <stop
                offset="100%"
                style={{stopColor: rightColorLegend, stopOpacity: 0.7}}
              />
            </linearGradient>
          </defs>
          <text
            x={marginLegend}
            y="20"
            style={{
              fontWeight: 'bold',
              fontSize: 10,
              fontFamily: 'sans-serif',
            }}
          >
            Ratio of points (%)
          </text>
          <rect
            x={marginLegend}
            y="25"
            width={legendWidth - marginLegend * 2}
            height="10"
            fill="url(#grad1)"
          />
        </svg>
      </div>
    );
  }

  render() {
    const style = {width: '100%', height: '400px'};
    return (
      <div style={{display: 'block'}}>
        <div className="mb-2 mt-2">
          {this.renderCoverageColumns(this.props.coverage)}
        </div>
        <div id={this.mapId} ref={this.mapRef} className="map" style={style} />
        <span className="mb-3" style={{fontSize: '0.9rem'}}>
          Left-click on bounding box to get more information.
        </span>
        {this.renderLegend()}
        <div ref={this.containerRef} className="ol-popup">
          <div ref={this.popupContentRef} />
        </div>
      </div>
    );
  }
}

export {GeoSpatialCoverageMap};
