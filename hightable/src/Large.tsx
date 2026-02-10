import Layout from './Layout'

import { HighTable } from 'hightable'
import { largeData } from './data'

export default function Large() {
  return <Layout>
    <HighTable data={largeData} cacheKey="demo" focus={false} />
  </Layout>
}
