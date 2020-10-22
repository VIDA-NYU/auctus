import React from 'react';

type initialPage = 1;

interface PaginationProps {
  items: Items[];
  onChangePage: (psgeOfItems: Items[]) => void;
  initialPage: initialPage;
  pageSize: number;
}

export interface Items {
  id: number;
  name: string;
}

interface PaginationState {
  pager: {
    totalItems: number;
    currentPage: number;
    pageSize: number;
    totalPages: number;
    startPage: number;
    endPage: number;
    startIndex: number;
    endIndex: number;
    pages: number[];
  };
}

class Pagination extends React.PureComponent<PaginationProps, PaginationState> {
  constructor(props: PaginationProps) {
    super(props);
    this.state = this.initialState();
  }
  static defaultProps = {
    initialPage: 1,
  };
  initialState() {
    return {
      pager: {
        totalItems: 0,
        currentPage: 0,
        pageSize: 0,
        totalPages: this.props.items.length,
        startPage: 0,
        endPage: 0,
        startIndex: 0,
        endIndex: 0,
        pages: [],
      },
    };
  }

  componentDidMount() {
    // set page if items array isn't empty
    if (this.props.items && this.props.items.length) {
      this.setPage(this.props.initialPage);
    }
  }

  componentDidUpdate(prevProps: PaginationProps) {
    // reset page if items array has changed
    if (this.props.items !== prevProps.items) {
      this.setPage(this.props.initialPage);
    }
  }

  setPage(page: number) {
    const {items, pageSize} = this.props;
    let pager = this.state.pager;

    if (page < 1 || page > pager.totalPages) {
      return;
    }
    // get new pager object for specified page
    pager = this.getPager(items.length, page, pageSize);

    // get new page of items from items array
    const pageOfItems = items.slice(pager.startIndex, pager.endIndex + 1);

    // update state
    this.setState({pager: pager});

    // call change page function in parent component
    this.props.onChangePage(pageOfItems);
  }

  getPager(totalItems: number, currentPage: number, pageSize: number) {
    // default to first page
    currentPage = currentPage || 1;

    // default page size is 10
    pageSize = pageSize || 10;

    // calculate total pages
    const totalPages = Math.ceil(totalItems / pageSize);

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

    // calculate start and end item indexes
    const startIndex = (currentPage - 1) * pageSize;
    const endIndex = Math.min(startIndex + pageSize - 1, totalItems - 1);

    // create an array of pages to ng-repeat in the pager control
    const idxs = Array.from(new Array(endPage + 1 - startPage).keys());
    const pages = idxs.map(i => startPage + i);

    // return object with all pager properties required by the view
    return {
      totalItems: totalItems,
      currentPage: currentPage,
      pageSize: pageSize,
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
