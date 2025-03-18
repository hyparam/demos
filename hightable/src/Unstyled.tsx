import { HighTable } from 'hightable'
import { useState } from 'react'
import { data } from './data'
import Layout from './Layout'

export default function Basic() {
  const [, setSelection] = useState<Selection | undefined>(undefined)

  return <Layout>
    <HighTable data={data} cacheKey="demo" styled={false} onSelectionChange={setSelection} />
  </Layout>
}
