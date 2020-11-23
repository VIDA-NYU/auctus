import React from 'react';
const defaultInitialPage = 1;

interface PaginationProps {
  totalRows: number;
  onChangePage: (pager: Pager) => void;
  initialPage: number;
  pageSize: number;
}

export interface Pager {
  currentPage: number;
  totalPages: number;
  startPage: number;
  endPage: number;
  startIndex: number;
  endIndex: number;
  pages: number[];
}

interface PaginationState {
  pager: Pager;
}

class Pagination extends React.PureComponent<PaginationProps, PaginationState> {
  constructor(props: PaginationProps) {
    super(props);
    this.state = this.initialState();
  }
  static defaultProps = {
    initialPage: defaultInitialPage,
  };
  initialState() {
    return {
      pager: {
        currentPage: 0,
        totalPages: this.props.totalRows
          ? Math.ceil(this.props.totalRows / this.props.pageSize)
          : 0,
        startPage: 0,
        endPage: 0,
        startIndex: 0,
        endIndex: 0,
        pages: [],
      },
    };
  }

  componentDidMount() {
    // set initial page
    if (this.props.totalRows) {
      this.setPage(this.props.initialPage);
    }
  }

  componentDidUpdate(prevProps: PaginationProps) {
    // reset page if the total number of rows has changed
    if (this.props.totalRows !== prevProps.totalRows) {
      this.setPage(this.props.initialPage);
    }
  }

  setPage(page: number) {
    const {totalRows, pageSize} = this.props;
    let pager = this.state.pager;

    if (page < 1 || page > pager.totalPages) {
      return;
    }
    // get new pager object for specified page
    pager = this.getPager(totalRows, page, pageSize);

    // update state
    this.setState({pager: pager});

    // call change page function in parent component
    this.props.onChangePage(pager);
  }

  getPager(totalRows: number, currentPage: number, pageSize: number) {
    // default to first page
    currentPage = currentPage || 1;

    // default page size is 10
    pageSize = pageSize || 10;

    // calculate total pages
    const totalPages = Math.ceil(totalRows / pageSize);

    let startPage = 0;
    let endPage = 0;
    const displayedPages = 5;
    const leftPages = displayedPages - Math.ceil(displayedPages / 2);
    if (totalPages <= displayedPages) {
      // less than 'displayedPages' total pages so show all
      startPage = 1;
      endPage = totalPages;
    } else {
      // more than 'displayedPages' total pages so calculate start and end pages
      if (currentPage <= Math.ceil(displayedPages / 2)) {
        startPage = 1;
        endPage = displayedPages;
      } else if (currentPage + leftPages >= totalPages) {
        startPage = totalPages - (displayedPages - 1);
        endPage = totalPages;
      } else {
        startPage = currentPage - leftPages;
        endPage = currentPage + leftPages;
      }
    }

    // calculate start and end row indexes
    const startIndex = (currentPage - 1) * pageSize;
    const endIndex = Math.min(startIndex + pageSize - 1, totalRows - 1);

    // create an array of pages to ng-repeat in the pager control
    const idxs = Array.from(new Array(endPage + 1 - startPage).keys());
    const pages = idxs.map(i => startPage + i);

    // return object with all pager properties required by the view
    return {
      currentPage: currentPage,
      totalPages: totalPages,
      startPage: startPage,
      endPage: endPage,
      startIndex: startIndex,
      endIndex: endIndex,
      pages: pages,
    };
  }

  render() {
    const pager = this.state.pager;
    if (!pager.pages || pager.pages.length <= 1) {
      // don't display pager if there is only 1 page
      return null;
    }

    return (
      <ul className="pagination">
        <li
          className={
            pager.currentPage === 1 ? 'disabled page-item' : 'page-item'
          }
        >
          <button
            type="submit"
            className="btn btn-primary page-link"
            onClick={() => this.setPage(1)}
          >
            First
          </button>
        </li>
        <li
          className={
            pager.currentPage === 1 ? 'page-item  disabled' : 'page-item '
          }
        >
          <button
            type="submit"
            className="btn btn-primary page-link"
            onClick={() => this.setPage(pager.currentPage - 1)}
          >
            Previous
          </button>
        </li>
        {pager.pages.map((page, index) => (
          <li
            key={index}
            className={
              pager.currentPage === page ? 'page-item active' : 'page-item'
            }
          >
            <button
              type="submit"
              className="btn btn-primary page-link"
              onClick={() => this.setPage(page)}
            >
              {page}
            </button>
          </li>
        ))}
        <li
          className={
            pager.currentPage === pager.totalPages
              ? 'page-item disabled'
              : 'page-item'
          }
        >
          <button
            type="submit"
            className="btn btn-primary page-link"
            onClick={() => this.setPage(pager.currentPage + 1)}
          >
            Next
          </button>
        </li>
        <li
          className={
            pager.currentPage === pager.totalPages
              ? 'page-item disabled'
              : 'page-item'
          }
        >
          <button
            type="submit"
            className="btn btn-primary page-link"
            onClick={() => this.setPage(pager.totalPages)}
          >
            Last
          </button>
        </li>
      </ul>
    );
  }
}

export default Pagination;
