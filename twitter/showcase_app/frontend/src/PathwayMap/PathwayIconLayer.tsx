import { IconLayer, TextLayer } from '@deck.gl/layers/typed';
import { CompositeLayer } from 'deck.gl/typed';
import { Position } from '@deck.gl/core/typed';
import { CompositeLayerProps } from '@deck.gl/core/typed';
import GL from '@luma.gl/constants';
import { GeofenceIconData, KINDS } from "./PathwayIconTypes"

type RGBAColor = [number, number, number, number];
const DEFAULT_HIGHLIGHT_COLOR: RGBAColor = [143, 215, 227, 180]
const DEFAULT_SELECTION_COLOR: RGBAColor = [30, 225, 137, 220]

const ICON_URL_PREFIX = 'assets/'
const SIZE_SCALE = 2 / 3 // https://deck.gl/docs/upgrade-guide#upgrading-from-deckgl-v84-to-v85

interface PathwayIconProps extends CompositeLayerProps<GeofenceIconData> {
    selectedIndices: Set<number>,
    staticSelectedIndices: Set<number>,
    highlightedIndices: Set<number>,
}

export type PProps<DataT> = PathwayIconProps &
    CompositeLayerProps<DataT>;

export default class PathwayIconLayer<DataT> extends CompositeLayer<PProps<DataT>> {
    renderLayers() {
        const data = this.props.data as GeofenceIconData[]
        return [
            new IconLayer<GeofenceIconData>({
                ...this.props,
                id: `${this.props.id}-icons`,
                alphaCutoff: 0.5,
                data: data,
                getIcon: (d: GeofenceIconData) => ({
                    url: ICON_URL_PREFIX + KINDS[d.kind].icon.name + '.png',
                    // url: 'https://w7.pngwing.com/pngs/124/929/png-transparent-gps-navigation-systems-global-positioning-system-computer-icons-gps-location-gps-navigation-systems-waypoint-symbol-thumbnail.png',
                    width: 128,
                    height: 128,
                    anchorY: KINDS[d.kind].icon.anchorY,
                    mask: true,
                }),
                pickable: true,
                opacity: 1,
                sizeUnits: 'pixels',
                getPosition: (d: GeofenceIconData) => {
                    let coord: Position = [d.coordinates[0], d.coordinates[1], d.coordinates[2]]
                    coord[2] += this.props.staticSelectedIndices.has(d.rowId) ? 50 : 0
                    coord[2] += this.props.selectedIndices.has(d.rowId) ? 50 : 0
                    coord[2] += this.props.highlightedIndices.has(d.rowId) ? 50 : 0
                    return coord
                },
                getSize: d => {
                    let multiplier = 1.0
                    if (this.props.highlightedIndices.has(d.rowId) || this.props.selectedIndices.has(d.rowId) || this.props.staticSelectedIndices.has(d.rowId)) {
                        multiplier *= 1.1
                    }
                    const kindMultiplier = KINDS[d.kind].sizeMultiplier
                    return d.size === 0 ? 0 : Math.max(15 * multiplier, Math.min(50, d.size * d.size) * kindMultiplier * multiplier)
                },
                getColor: (d: GeofenceIconData) => {
                    if (this.props.highlightedIndices.has(d.rowId)) {
                        return DEFAULT_HIGHLIGHT_COLOR
                    }
                    if (this.props.selectedIndices.has(d.rowId)) {
                        // return DEFAULT_SELECTION_COLOR
                        return [...KINDS[d.kind].color].map((c, idx) => Math.max(c * 1.4, c + 40)) as RGBAColor
                    }
                    return KINDS[d.kind].color
                },
                sizeScale: SIZE_SCALE,
                sizeMaxPixels: 150,
                getPolygonOffset: () => [0, -1000],
            }),
            new TextLayer<GeofenceIconData>({
                ...this.props,
                id: `${this.props.id}-text`,
                data: data,
                pickable: true,
                opacity: 1,
                outlineWidth: 2,
                //@ts-ignore: coordinates can be also 3D
                getPosition: (d: GeofenceIconData) => {
                    let coord: Position = [d.coordinates[0], d.coordinates[1], d.coordinates[2] + 50]
                    coord[2] += this.props.staticSelectedIndices.has(d.rowId) ? 50 : 0
                    coord[2] += this.props.selectedIndices.has(d.rowId) ? 50 : 0
                    coord[2] += this.props.highlightedIndices.has(d.rowId) ? 50 : 0
                    return coord
                },
                getSize: d => {
                    return Math.max(20, 2 * d.size - 10)
                },
                getText: d => d.text,
                sizeScale: SIZE_SCALE,
                getColor: d => [50, 50, 50, 200],
                fontFamily: '"Source Sans Pro", sans-serif',
                getTextAnchor: 'middle',
                getAlignmentBaseline: 'center',
                getPixelOffset: d => KINDS[d.kind].textPixelsOffset,
                getPolygonOffset: () => [0, -1000],
            })
        ]
    }
}

PathwayIconLayer.layerName = 'PathwayIconLayer'