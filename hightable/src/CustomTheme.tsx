import { HighTable } from 'hightable'
import { useState } from 'react'
import './CustomTheme.css'
import { data } from './data'
import Layout from './Layout'

export default function CustomTheme() {
  const [, setSelection] = useState<Selection | undefined>(undefined)

  return <Layout>
    <HighTable data={data} cacheKey="demo" styled={true} onSelectionChange={setSelection} className="custom-hightable" />
  </Layout>
}
