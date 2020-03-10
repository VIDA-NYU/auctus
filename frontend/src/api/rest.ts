import axios, { AxiosResponse } from 'axios';
import { SearchResponse } from './types';
import { API_URL } from '../config';

export enum ResquestResult {
  SUCCESS = 'SUCCESS',
  ERROR = 'ERROR',
}

export interface Response<T> {
  status: ResquestResult;
  data?: T;
}

export interface QuerySpec {
  keywords: string[];
  source: string[];
  variables: Array<{}>;
}

function parseQueryString(q?: string): string[] {
  return q ? q.split(' ').filter(t => t.length > 0) : [];
}

export async function search(
  query?: string
): Promise<Response<SearchResponse>> {
  const url = `${API_URL}/search?_parse_sample=1`;

  const spec: QuerySpec = {
    keywords: parseQueryString(query),
    source: [
      'data.baltimorecity.gov',
      'data.cityofchicago.org',
      'data.cityofnewyork.us',
      'data.ny.gov',
      'data.sfgov.org',
      'data.wa.gov',
      'finances.worldbank.org',
      'upload',
    ],
    variables: [],
  };

  const formData = new FormData();
  formData.append('query', JSON.stringify(spec));

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
