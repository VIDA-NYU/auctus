import axios, { AxiosResponse, AxiosRequestConfig } from 'axios';
import { SearchResponse, SearchResult, FilterVariables, QuerySpec } from './types';
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

export enum RequestResult {
  SUCCESS = 'SUCCESS',
  ERROR = 'ERROR',
}

export interface Response<T> {
  status: RequestResult;
  data?: T;
}

export interface SearchQuery {
  query?: string,
  filters?: FilterVariables[],
  sources?: string[],
  file?: File,
}

function parseQueryString(q?: string): string[] {
  return q ? q.split(' ').filter(t => t.length > 0) : [];
}

export async function search(q: SearchQuery): Promise<Response<SearchResponse>> {
  const url = `${API_URL}/search?_parse_sample=1`;

  const spec: QuerySpec = {
    keywords: parseQueryString(q.query),
    source: q.sources && q.sources.length > 0 ? q.sources : DEFAULT_SOURCES,
    variables: q.filters ? [...q.filters] : [],
  };

  const formData = new FormData();
  formData.append('query', JSON.stringify(spec));
  if (q.file) {
    formData.append('data', q.file);
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
        status: RequestResult.SUCCESS,
        data: response.data,
      };
    })
    .catch(error => {
      return {
        status: RequestResult.ERROR,
      };
    });
}

export function augment(data: File, task: SearchResult): Promise<Response<Blob>> {
  const formData = new FormData();
  formData.append('data', data);
  formData.append('task', JSON.stringify(task));

  const url = `${API_URL}/augment`;
  const config: AxiosRequestConfig = {
    responseType: 'blob',
    headers: {
      'content-type': 'multipart/form-data',
    },
  };
  return axios
    .post(url, formData, config)
    .then((response: AxiosResponse) => {
      if(response.status !== 200) {
        throw Error("Status " + response.status);
      }
      return {
        status: RequestResult.SUCCESS,
        data: response.data,
      };
    })
    .catch(error => {
      return {
        status: RequestResult.ERROR,
      };
    });
}
