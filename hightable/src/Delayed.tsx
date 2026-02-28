import Layout from './Layout'

import { HighTable } from 'hightable'
import { delayed } from './data'

export default function Delayed() {
  return <Layout>
    <HighTable data={delayed} cacheKey="demo" focus={false} />
  </Layout>
}
