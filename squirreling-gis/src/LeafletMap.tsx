import type { Geometry } from 'hyparquet/src/types.js'
import L from 'leaflet'
import { ReactNode, useEffect, useRef } from 'react'

export interface MapFeature {
  geometry: Geometry
  properties: Record<string, unknown>
}

interface LeafletMapProps {
  features: MapFeature[]
}

/**
 * Leaflet map component that renders GeoJSON features.
 */
export default function LeafletMap({ features }: LeafletMapProps): ReactNode {
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef = useRef<L.Map | null>(null)
  const layerRef = useRef<L.GeoJSON | null>(null)

  // Initialize map once
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return

    const map = L.map(containerRef.current).setView([20, 0], 2)

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
      maxZoom: 19,
    }).addTo(map)

    mapRef.current = map

    return () => {
      map.remove()
      mapRef.current = null
    }
  }, [])

  // Update features when they change
  useEffect(() => {
    const map = mapRef.current
    if (!map) return

    // Remove previous layer
    if (layerRef.current) {
      map.removeLayer(layerRef.current)
      layerRef.current = null
    }

    if (features.length === 0) return

    // Build GeoJSON FeatureCollection
    const geojson = {
      type: 'FeatureCollection' as const,
      features: features.map(f => ({
        type: 'Feature' as const,
        geometry: f.geometry,
        properties: f.properties,
      })),
    }

    const layer = L.geoJSON(geojson, {
      style: {
        color: '#4433aa',
        weight: 2,
        opacity: 0.8,
        fillColor: '#6450b4',
        fillOpacity: 0.3,
      },
      pointToLayer(_feature, latlng) {
        return L.circleMarker(latlng, {
          radius: 5,
          color: '#4433aa',
          weight: 2,
          opacity: 0.8,
          fillColor: '#6450b4',
          fillOpacity: 0.5,
        })
      },
      onEachFeature(_feature, featureLayer) {
        const props = _feature.properties as Record<string, unknown> | null
        if (!props || Object.keys(props).length === 0) return
        const rows = Object.entries(props)
          .map(([k, v]) => {
            const str = typeof v === 'string' ? v : JSON.stringify(v)
            const val = v === null ? '<i>null</i>' : escapeHtml(str)
            return `<tr><td><b>${escapeHtml(k)}</b></td><td>${val}</td></tr>`
          })
          .join('')
        featureLayer.bindPopup(`<table class="popup-table">${rows}</table>`, { maxWidth: 400 })
      },
    }).addTo(map)

    layerRef.current = layer

    // Fit map to data bounds
    try {
      const bounds = layer.getBounds()
      if (bounds.isValid()) {
        map.fitBounds(bounds, { padding: [20, 20] })
      }
    } catch {
      // bounds may fail if no valid geometries
    }
  }, [features])

  return <div ref={containerRef} className="leaflet-container-wrapper" />
}

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}
