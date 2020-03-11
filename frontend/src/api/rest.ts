import axios, { AxiosResponse } from 'axios';
import { SearchResponse } from './types';
import { API_URL } from '../config';

export const DEFAULT_SOURCES = [
  'data.baltimorecity.gov',
  'data.cityofchicago.org',
  'data.cityofnewyork.us',
  'data.ny.gov',
  'data.sfgov.org',
  'data.wa.gov',
  'finances.worldbank.org',
  'upload',
];

export enum ResquestResult {
  SUCCESS = 'SUCCESS',
  ERROR = 'ERROR',
}

export interface Response<T> {
  status: ResquestResult;
  data?: T;
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

function parseQueryString(q?: string): string[] {
  return q ? q.split(' ').filter(t => t.length > 0) : [];
}

export async function search(
  query?: string,
  filters?: FilterVariables[],
  sources?: string[],
  file?: File,
): Promise<Response<SearchResponse>> {
  const url = `${API_URL}/search?_parse_sample=1`;

  const spec: QuerySpec = {
    keywords: parseQueryString(query),
    source: sources && sources.length > 0 ? sources : DEFAULT_SOURCES,
    variables: filters ? [...filters] : [],
  };

  const formData = new FormData();
  formData.append('query', JSON.stringify(spec));
  if (file) {
    formData.append('data', file);
  }
  const config = {
    headers: {
      'content-type': 'multipart/form-data',
    },
  };

  return axios
    .post(url, formData, config)
    .then((response: AxiosResponse) => {
      return {
        status: ResquestResult.SUCCESS,
        data: response.data,
      };
    })
    .catch(error => {
      return {
        status: ResquestResult.ERROR,
      };
    });
}
