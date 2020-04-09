export interface AugmentationTask {
  data: SearchResult;
}

export interface AugmentationInfo {
  type: string;
  left_columns: number[][];
  left_columns_names: string[][];
  right_columns: number[][];
  right_columns_names: string[][];
}

export interface SpatialCoverage {
  lat?: string;
  lon?: string;
  address?: string;
  ranges: Array<{
    range: {
      coordinates: [[number, number], [number, number]];
      type: 'envelope';
    };
  }>;
}

export interface Metadata {
  filename: string;
  name: string;
  description: string;
  size: number;
  nb_rows: number;
  columns: ColumnMetadata[];
  date: string;
  materialize: {};
  nb_profiled_rows: number;
  sample: string;
  source: string;
  version: string;
  spatial_coverage: SpatialCoverage[];
}

export interface ColumnMetadata {
  name: string;
  num_distinct_values: number;
  structural_type: string;
  semantic_types: string[];
}

export interface SearchResult {
  id: string;
  score: number;
  // join_columns: Array<[string, string]>;
  metadata: Metadata;
  augmentation?: AugmentationInfo;
  supplied_id: string;
  supplied_resource_id: string;
  d3m_dataset_description: {};
  sample: string[][];
}

export interface SearchResponse {
  results: SearchResult[];
}

export interface Variable {
  type: string;
}

export interface TemporalVariable {
  type: 'temporal_variable';
  start?: string;
  end?: string;
}

export interface GeoSpatialVariable {
  type: 'geospatial_variable';
  latitude1: string;
  longitude1: string;
  latitude2: string;
  longitude2: string;
}

export type FilterVariables = TemporalVariable | GeoSpatialVariable;

export interface QuerySpec {
  keywords: string[];
  source: string[];
  variables: FilterVariables[];
}
