import * as React from 'react';
import {
  Hits,
  HitsStats,
  Pagination,
  RefinementListFilter,
  ResetFilters,
  SearchBox,
  SearchkitManager,
  SearchkitProvider,
} from "searchkit";
import 'searchkit/release/theme.css'
import './App.css';

const sk = new SearchkitManager("http://localhost:9200/datamart/", {})

const HitItem = (props: any) => (
  <div className={"row"}>
    <div className="col-md-12 excerpet mt-3">
      <div><span><span className={"font-weight-bold"}>Dataset:</span> {props.result._source.materialize.d3m_name}</span></div>
      <div><span><span className={"font-weight-bold"}>License:</span> {props.result._source.license}</span></div>
      <div><span><span className={"font-weight-bold"}>Description:</span> {props.result._source.description}</span></div>
    </div>
  </div>
)

class App extends React.Component {
  public render() {
    return (
      <div className="App">
        <nav className="navbar navbar-expand-lg navbar-dark bg-primary" id="special-nav">
          <div>
            <a className="navbar-brand special-brand" href="./">Datamart Search</a>
          </div>
        </nav>
        <SearchkitProvider searchkit={sk}>
          <div className="row">
            <div className="col col-md-2 ml-2 mr-2" >
              <RefinementListFilter id="license" field="license.keyword" title="License" size={10} />
            </div>
            <div className="col col-md-8 pl-5">
              <div className="mt-3">
                <SearchBox
                  searchOnChange={true}
                  placeholder="Search datasets..."
                  searchThrottleTime={1000}
                  queryFields={["_id", "materialize.d3m_name", "license", "description"]}
                />
                <ResetFilters />
                <HitsStats translations={{"hitstats.results_found":"{hitCount} results found."}}/>
              </div>
              <Hits
                hitsPerPage={10}
                highlightFields={["description"]}
                sourceFilter={["_id", "materialize.d3m_name", "license", "date", "description"]}
                itemComponent={HitItem}
              />
              <Pagination showNumbers={true}/>
            </div>
          </div>
        </SearchkitProvider>
      </div>
    );
  }
}

export default App;
