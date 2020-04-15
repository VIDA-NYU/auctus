import axios, { AxiosResponse, AxiosRequestConfig } from 'axios';
import {
  SearchResponse,
  SearchResult,
  FilterVariables,
  QuerySpec,
} from './types';
import { API_URL } from '../config';

const api = axios.create({
  baseURL: API_URL,
});

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
  query?: string;
  filters?: FilterVariables[];
  sources?: string[];
  file?: File;
}

function parseQueryString(q?: string): string[] {
  return q ? q.split(' ').filter(t => t.length > 0) : [];
}

export function search(q: SearchQuery): Promise<Response<SearchResponse>> {
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

  return api
    .post('/search?_parse_sample=1', formData, config)
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

export function augment(
  data: File,
  task: SearchResult
): Promise<Response<Blob>> {
  const formData = new FormData();
  formData.append('data', data);
  formData.append('task', JSON.stringify(task));

  const config: AxiosRequestConfig = {
    responseType: 'blob',
    headers: {
      'content-type': 'multipart/form-data',
    },
  };
  return api
    .post('/augment', formData, config)
    .then((response: AxiosResponse) => {
      if (response.status !== 200) {
        throw Error('Status ' + response.status);
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

export interface UploadData {
  name: string;
  description?: string;
  address?: string;
  file?: File;
}

export function upload(data: UploadData) {
  const formData = new FormData();
  formData.append('name', data.name);

  if (data.description) {
    formData.append('description', data.description);
  }

  if (data.address) {
    formData.append('address', data.address);
  } else if (data.file) {
    formData.append('file', data.file);
  }

  const config: AxiosRequestConfig = {
    maxRedirects: 0,
    headers: {
      'content-type': 'multipart/form-data',
    },
  };

  return api.post('/upload', formData, config);
}

export interface RecentDiscovery {
  id: string;
  discoverer: string;
  discovered: Date;
  profiled: Date;
  name: string;
  spatial?: boolean;
  temporal?: boolean;
}

export interface Status {
  recent_discoveries: RecentDiscovery[];
  sources_counts: {
    [source: string]: number;
  };
}

export async function status(): Promise<Status> {
  const response = await api.get('/statistics');
  return response.data;
}
