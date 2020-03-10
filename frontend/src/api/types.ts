export interface AugmentationInfo {
  type: string;
  left_columns: number[][];
  left_columns_names: string[][];
  right_columns: number[][];
  right_columns_names: string[][];
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
}

export interface SearchResponse {
  results: SearchResult[];
}
