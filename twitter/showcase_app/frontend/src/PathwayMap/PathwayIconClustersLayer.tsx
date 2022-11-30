import { CompositeLayer } from 'deck.gl/typed';
import { CompositeLayerProps } from '@deck.gl/core/typed';
import Supercluster from 'supercluster';
import PathwayIconLayer from "./PathwayIconLayer"
import { GeofenceData, GeofenceIconData } from "./PathwayIconTypes"

function easeInOutCubic(x: number) {
    return x < 0.5 ? 4 * x * x * x : 1 - Math.pow(-2 * x + 2, 3) / 2;
}

const TRANSITION_DURATION = 400;

interface SuperclusterData {
    geometry: {
        coordinates: [number, number],
    }
    properties: GeofenceData & { cluster?: boolean },
    type: any
}

interface Cluster {
    geometry: {
        coordinates: any
    },
    properties: GeofenceData & {
        cluster_id: number,
        cluster?: boolean,
        point_count_abbreviated?: any
    }
}

interface Props extends CompositeLayerProps<GeofenceData> {
    selectedIndices: Set<number>,
    staticSelectedIndices: Set<number>,
    highlightedIndices: Set<number>
    sizeScale?: number
    showNumbers: boolean
    sizeFactor: number
}

export type PProps<DataT> = Props &
    CompositeLayerProps<DataT>;

/**
 * 
 * Layer to show clustered icons based on zoom level.
 *
 * It always plots all data, but with size=0 for hidden stuff instead of filtering
 * Thanks to this, deckgl can properly handle animations (transitions)
 * i.e. to visualize that the icon belongs to given parent cluster.
 */
export default class PathwayIconClustersLayer<DataT> extends CompositeLayer<PProps<DataT>> {
    /**
     * Necessary for zoom refreshing to work
     */
    shouldUpdateState({ changeFlags }: any): any {
        return changeFlags.somethingChanged;
    }

    updateState({ oldProps, props, changeFlags }: { oldProps: Props, props: Props, changeFlags: any }) {
        let rebuildClusterIndex = changeFlags.dataChanged || props.sizeScale !== oldProps.sizeScale
        const data = this.props.data as GeofenceData[]
        if (rebuildClusterIndex) {
            const clusterIndex = new Supercluster({ maxZoom: 16, radius: 20, minPoints: 3 })
            const superclusterData: any[] = data.map(d => ({
                geometry: { coordinates: d.coordinates.slice(0, 2) },
                properties: d,
                type: null
            }))
            clusterIndex.load(superclusterData);
            this.setState({ clusterIndex })
        }
        const z = Math.floor(this.context.viewport.zoom);
        if (rebuildClusterIndex
            || z !== this.state.z
            || props.selectedIndices !== oldProps.selectedIndices
            || props.highlightedIndices !== oldProps.highlightedIndices
        ) {
            this.refreshZoom(z)
        }
    }

    protected getClusterRepresentative(leaves: SuperclusterData[]) {
        const bestLeaf = leaves.sort(
            (a, b) =>
                (a.properties.visible ? 100000 : 0) - (b.properties.visible ? 100000 : 0)
                + (a.properties.confidence === 'certain' ? 1000 : 0) - (b.properties.confidence === 'certain' ? 1000 : 0)
                + a.properties.importance - b.properties.importance
        ).pop()
        if (bestLeaf) {
            return bestLeaf.properties
        } else {
            throw new Error("Empty cluster.")
        }
    }

    protected isSpecialGeofence(row: GeofenceData): boolean {
        return this.props.selectedIndices?.has(row.rowId) ||
            this.props.staticSelectedIndices?.has(row.rowId) ||
            this.props.highlightedIndices?.has(row.rowId)
    }

    protected refreshZoom(z: number) {
        const clusterIndex = this.state.clusterIndex
        const clusters: Cluster[] = clusterIndex.getClusters([-180, -85, 180, 85], z)
        const dataForCurrentZ = new Map<number, GeofenceIconData>()
        for (const cluster of clusters) {
            // check if not singleton
            if (cluster.properties.cluster) {
                const leaves: SuperclusterData[] = clusterIndex.getLeaves(cluster.properties.cluster_id, Infinity)
                let count = leaves.length
                const sizeSum = leaves.reduce((agg, val) => agg + Number(val.properties.visible) * val.properties.size, 0)
                const parent = this.getClusterRepresentative(leaves)

                for (const leafIcon of leaves) {
                    const row = leafIcon.properties
                    if (!row.visible) {
                        // hidden geofence
                        dataForCurrentZ.set(row.rowId, {
                            ...row,
                            size: 0,
                            text: '',
                            visible: false
                        })
                        count -= 1
                        continue
                    }
                    if (this.isSpecialGeofence(row)) {
                        // highlighted/selected geofence: take out from cluster
                        dataForCurrentZ.set(row.rowId, {
                            ...row,
                            text: '',
                            visible: true
                        })
                        count -= 1
                    } else {
                        // leaf geofence: hide and show only representative
                        dataForCurrentZ.set(row.rowId, {
                            ...row,
                            size: 0,
                            text: '',
                            visible: false,
                            // coordinates: parent.coordinates
                            coordinates: cluster.geometry.coordinates
                        })
                        if (row.size == 0) {
                            count -= 1
                        }
                    }
                }
                if (parent.visible) {
                    // show cluster representative
                    dataForCurrentZ.set(parent.rowId, {
                        ...parent,
                        // size: parent.size,
                        size: sizeSum === 0 ? 0 : Math.log(sizeSum) * this.props.sizeFactor,
                        text: this.props.showNumbers && count > 1 ? count.toString() : '',
                        // text: sizeSum.toString(),
                        // text: importanceSum.toString(),
                        visible: true,
                        coordinates: parent.coordinates,
                        // coordinates: cluster.geometry.coordinates
                    })
                } else {
                    dataForCurrentZ.set(parent.rowId, { ...parent, size: 0, visible: false, text: '' })
                }
            } else {
                const singleton: GeofenceData = cluster.properties
                if (singleton.visible) {
                    dataForCurrentZ.set(singleton.rowId, { ...singleton, size: singleton.size === 0 ? 0 : Math.log(singleton.size) * this.props.sizeFactor, text: singleton.confidence == 'certain' ? '' : '?', visible: true })
                } else {
                    dataForCurrentZ.set(singleton.rowId, { ...singleton, text: '', visible: false, size: 0 })
                }
            }
        }
        const dataSortedById = Array.from(dataForCurrentZ.values()).sort((a, b) => a.rowId - b.rowId)
        this.setState({ z, dataForCurrentZ: dataSortedById })
    }

    renderLayers() {
        return [
            new PathwayIconLayer({
                id: `${this.props.id}-clusters`,
                data: this.state.dataForCurrentZ,
                transitions: {
                    // animations disabled due to jumping in streaming mode
                    // getPosition: { duration: TRANSITION_DURATION, easing: easeInOutCubic, type: "interpolation" },
                    // getSize: { duration: TRANSITION_DURATION, easing: easeInOutCubic, type: "interpolation" },
                    // getColor: { duration: TRANSITION_DURATION, easing: easeInOutCubic, type: "interpolation" },
                },
                staticSelectedIndices: this.props.staticSelectedIndices,
                selectedIndices: this.props.selectedIndices,
                highlightedIndices: this.props.highlightedIndices,
            })
        ]

    }
}

PathwayIconClustersLayer.layerName = 'PathwayIconClustersLayer'