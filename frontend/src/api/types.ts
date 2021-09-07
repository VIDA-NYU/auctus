export interface AugmentationTask {
  data: SearchResult;
}

export interface ColumnAggregations {
  [columnName: string]: string[];
}

// Keep in sync with datamart_profiler's temporal_aggregation_keys
export enum TemporalResolution {
  YEAR = 'year',
  QUARTER = 'quarter',
  MONTH = 'month',
  WEEK = 'week',
  DAY = 'day',
  HOUR = 'hour',
  MINUTE = 'minute',
  SECOND = 'second',
}

export enum AugmentationType {
  JOIN = 'join',
  UNION = 'union',
  NONE = 'none',
}

export interface AugmentationInfo {
  type?: AugmentationType;
  left_columns: number[][];
  left_columns_names: string[][];
  right_columns: number[][];
  right_columns_names: string[][];
  agg_functions?: ColumnAggregations;
  temporal_resolution?: TemporalResolution;
}

export interface SpatialCoverage {
  // Keep in sync, search code for 279a32
  column_names: string[];
  column_indexes: number[];
  type: string;
  ranges: Array<{
    range: {
      coordinates: [[number, number], [number, number]];
      type: 'envelope';
    };
  }>;
  geohashes4?: Array<{hash: string; number: number}>;
  number?: number;
}

export interface Metadata {
  id: string;
  filename?: string;
  name: string;
  description: string;
  size: number;
  nb_rows: number;
  columns: ColumnMetadata[];
  date: string;
  materialize: {date: string};
  nb_profiled_rows: number;
  sample: string;
  source: string;
  source_url?: string;
  types: string[];
  version: string;
  spatial_coverage?: SpatialCoverage[];
}

export interface ColumnMetadata {
  name: string;
  structural_type: string;
  semantic_types: string[];
  num_distinct_values?: number;
  coverage?: Array<unknown>;
  mean?: number;
  stddev?: number;
  plot?: PlotVega;
  temporal_resolution?: string;
  admin_area_level?: number;
  point_format?: 'lat,long' | 'long,lat';
  latlong_pair?: string;
}

export interface PlotVega {
  type: string;
  data:
    | NumericalDataVegaFormat[]
    | TemporalDataVegaFormat[]
    | CategoricalDataVegaFormat[];
}

export interface SearchResult {
  id: string;
  score: number;
  // join_columns: Array<[string, string]>;
  metadata: Metadata;
  augmentation?: AugmentationInfo;
  sample: string[][];
}

export interface SearchFacet {
  buckets: {[bucket: string]: number};
  incomplete: boolean;
}

export interface SearchFacets {
  source: SearchFacet;
  license: SearchFacet;
  type: SearchFacet;
}

export interface SearchResponse {
  results: SearchResult[];
  facets?: SearchFacets;
  total?: number;
}

export interface Variable {
  type: string;
}

export interface TemporalVariable {
  type: 'temporal_variable';
  start?: string;
  end?: string;
  granularity?: string;
}

export interface GeoSpatialVariable {
  type: 'geospatial_variable';
  latitude1: number;
  longitude1: number;
  latitude2: number;
  longitude2: number;
}

export interface TabularVariable {
  type: 'tabular_variable';
  columns: number[];
  relationship: string;
}

export type FilterVariables =
  | TabularVariable
  | TemporalVariable
  | GeoSpatialVariable;

export interface QuerySpec {
  keywords?: string;
  source?: string[];
  types?: string[];
  variables: FilterVariables[];
  augmentation_type?: AugmentationType;
}

interface RelatedToFileBase {
  kind: string;
  name: string;
  fileSize?: number;
}

export interface RelatedToLocalFile extends RelatedToFileBase {
  kind: 'localFile';
  token: string;
  tabularVariables?: TabularVariable;
}

export interface RelatedToSearchResult extends RelatedToFileBase {
  kind: 'searchResult';
  datasetId: string;
  tabularVariables?: TabularVariable;
}

export type RelatedFile = RelatedToLocalFile | RelatedToSearchResult;

export interface NumericalDataVegaFormat {
  count: number;
  bin_start: number;
  bin_end: number;
}

export interface TemporalDataVegaFormat {
  count: number;
  date_start: string;
  date_end: string;
}

export interface CategoricalDataVegaFormat {
  count: number;
  bin: string;
}

export enum InfoBoxType {
  DETAIL = 'DETAIL',
  AUGMENTATION = 'AUGMENTATION',
}

export enum Annotation {
  ADD = 'ADD',
  REMOVE = 'REMOVE',
}

export enum TypesCategory {
  STRUCTURAL = 'STRUCTURAL',
  SEMANTIC = 'SEMANTIC',
}

export interface Session {
  session_id: string;
  format?: string;
  format_options?: {[key: string]: string | number};
  data_token?: string;
  system_name: string;
}

export enum ColumnType {
  MISSING_DATA = 'https://metadata.datadrivendiscovery.org/types/MissingData',
  INTEGER = 'http://schema.org/Integer',
  FLOAT = 'http://schema.org/Float',
  TEXT = 'http://schema.org/Text',
  BOOLEAN = 'http://schema.org/Boolean',
  LATITUDE = 'http://schema.org/latitude',
  LONGITUDE = 'http://schema.org/longitude',
  DATE_TIME = 'http://schema.org/DateTime',
  ADDRESS = 'http://schema.org/address',
  ADMIN = 'http://schema.org/AdministrativeArea',
  ID = 'http://schema.org/identifier',
  CATEGORICAL = 'http://schema.org/Enumeration',
  GEO_POINT = 'http://schema.org/GeoCoordinates',
  GEO_POLYGON = 'http://schema.org/GeoShape',
}
