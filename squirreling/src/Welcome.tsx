import type { ReactNode } from 'react'

export default function Welcome(): ReactNode {

  return <div id="welcome">
    <div>
      <h1>Squirreling</h1>
      <h2>Async SQL engine for Parquet files</h2>
      <div className='badges'>
        <a href="https://www.npmjs.com/package/squirreling"><img src="https://img.shields.io/npm/v/squirreling" alt="npm squirreling" /></a>
        <a href="https://github.com/hyparam/squirreling"><img src="https://img.shields.io/github/stars/hyparam/squirreling?style=social" alt="star squirreling" /></a>
      </div>
      <p>
        Online demo of <a href="https://github.com/hyparam/squirreling">squirreling</a>: a library for building full text search indexes
        against parquet files stored in cloud object storage (S3, Azure Blob Storage, etc).
      </p>
      <p>
        This demo uses <a href="https://github.com/hyparam/hightable">hightable</a> for high performance table viewing.
      </p>
      <p>
        Example file:
      </p>
      <ul className="quick-links">
        <li>
          <a
            className="aws"
            href="?key=https://s3.hyperparam.app/parquetindex/wiki_en100.parquet">
            s3://hyperparam-public/parquetindex/wiki_en.parquet
          </a>
        </li>
      </ul>
    </div>
  </div>
}
