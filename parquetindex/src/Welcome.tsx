import type { ReactNode } from 'react'

export default function Welcome(): ReactNode {

  return <div id="welcome">
    <div>
      <h1>parquetindex</h1>
      <h2>Full text search against cloud-stored parquet files</h2>
      <div className='badges'>
        <a href="https://www.npmjs.com/package/parquetindex"><img src="https://img.shields.io/npm/v/parquetindex" alt="npm parquetindex" /></a>
        <a href="https://github.com/hyparam/parquetindex"><img src="https://img.shields.io/github/stars/hyparam/parquetindex?style=social" alt="star parquetindex" /></a>
      </div>
      <p>
        Online demo of <a href="https://github.com/hyparam/parquetindex">parquetindex</a>: a library for building full text search indexes
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
