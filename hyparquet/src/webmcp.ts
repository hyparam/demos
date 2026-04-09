// WebMCP Integration for hyparquet demo
// Exposes tools for AI agents to interact with the parquet viewer

declare global {
  interface Window {
    __hyparquetApp?: {
      getCurrentFile: () => { name: string; url?: string } | null
      loadUrl: (url: string) => void
      getAvailableExamples: () => Array<{ name: string; url: string }>
    }
  }
}

const exampleFiles = [
  { name: 'wiki-en-00000-of-00041.parquet', url: 'https://hyperparam-public.s3.amazonaws.com/wiki-en-00000-of-00041.parquet' },
  { name: 'starcoderdata-js-00000-of-00065.parquet', url: 'https://hyperparam.blob.core.windows.net/hyperparam/starcoderdata-js-00000-of-00065.parquet' },
  { name: 'github-code-00000-of-01126.parquet', url: 'https://huggingface.co/datasets/codeparrot/github-code/resolve/main/data/train-00000-of-01126.parquet?download=true' },
  { name: 'rowgroups.parquet', url: 'https://raw.githubusercontent.com/hyparam/hyparquet/master/test/files/rowgroups.parquet' },
]

// Store for current app state
let appState: {
  currentFile: { name: string; url?: string } | null
  loadUrlFn: ((url: string) => void) | null
} = {
  currentFile: null,
  loadUrlFn: null,
}

export function setCurrentFile(name: string, url?: string) {
  appState.currentFile = { name, url }
}

export function setLoadUrlFn(fn: (url: string) => void) {
  appState.loadUrlFn = fn
}

export function initWebMCP() {
  // Expose app API for tools
  window.__hyparquetApp = {
    getCurrentFile: () => appState.currentFile,
    loadUrl: (url: string) => {
      if (appState.loadUrlFn) {
        appState.loadUrlFn(url)
      } else {
        console.warn('[WebMCP] loadUrl function not available')
      }
    },
    getAvailableExamples: () => exampleFiles,
  }

  // Wait for WebMCP to be available (from CDN script)
  const registerTools = () => {
    if (!('modelContext' in navigator)) {
      console.log('[WebMCP] navigator.modelContext not available, retrying...')
      setTimeout(registerTools, 100)
      return
    }

    const mc = navigator.modelContext

    // Tool: Get current file info
    mc.registerTool({
      name: 'hyparquet_get_current_file',
      description: 'Get information about the currently loaded parquet file',
      inputSchema: {
        type: 'object',
        properties: {},
      },
      async execute() {
        const file = appState.currentFile
        if (!file) {
          return {
            content: [{ type: 'text', text: 'No file is currently loaded. Use hyparquet_load_url to load a parquet file.' }],
          }
        }
        return {
          content: [{ type: 'text', text: JSON.stringify(file, null, 2) }],
        }
      },
    })

    // Tool: Load a parquet URL
    mc.registerTool({
      name: 'hyparquet_load_url',
      description: 'Load a parquet file from a URL into the viewer',
      inputSchema: {
        type: 'object',
        properties: {
          url: {
            type: 'string',
            description: 'URL of the parquet file to load',
          },
        },
        required: ['url'],
      },
      async execute({ url }: { url: string }) {
        if (!appState.loadUrlFn) {
          return {
            content: [{ type: 'text', text: 'Error: hyparquet viewer not ready' }],
            isError: true,
          }
        }
        appState.loadUrlFn(url)
        return {
          content: [{ type: 'text', text: `Loading parquet file: ${url}` }],
        }
      },
    })

    // Tool: List available example files
    mc.registerTool({
      name: 'hyparquet_list_examples',
      description: 'List available example parquet files that can be loaded',
      inputSchema: {
        type: 'object',
        properties: {},
      },
      async execute() {
        return {
          content: [{ type: 'text', text: JSON.stringify(exampleFiles, null, 2) }],
        }
      },
    })

    console.log('[WebMCP] Tools registered successfully')
  }

  // Start registration
  registerTools()
}

export function getAppState() {
  return appState
}
