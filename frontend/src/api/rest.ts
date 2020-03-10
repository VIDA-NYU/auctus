import axios, { AxiosResponse } from 'axios';

const BASE_API_URL = 'https://auctus.vida-nyu.org';

enum ResponseStatus {
  SUCCESS = 'SUCCESS',
  ERROR = 'ERROR',
}

interface Response<T> {
  status: ResponseStatus;
  data?: T;
}

export interface QuerySpec {
  query: string[];
  source: string[];
  variables: Array<{}>;
}

export interface SearchResult {
  results: [
    {
      id: string;
      score: number;
      metadata: {
        columns: [
          {
            name: string; //"NAME",
            num_distinct_values: number; // 13626,
            semantic_types: string[]; // [],
            structural_type: string; // "http://schema.org/Text"
          }
        ];
      };
      // ...
    }
  ];
}

function parseQueryString(q?: string): string[] {
  return q ? q.split(' ').filter(t => t.length > 0) : [];
}

export async function search(query?: string): Promise<Response<SearchResult>> {
  const url = `${BASE_API_URL}/search?_parse_sample=1`;

  const spec = {
    query: parseQueryString(query),
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
        status: ResponseStatus.SUCCESS,
        data: response.data,
      };
    })
    .catch(error => {
      return {
        status: ResponseStatus.ERROR,
      };
    });
}
