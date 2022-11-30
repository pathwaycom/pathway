/// app.js
import React, { useEffect, useState, useMemo } from "react";

import { MapboxOverlay } from '@deck.gl/mapbox/typed';
import { useControl } from 'react-map-gl';
import Map, { NavigationControl } from 'react-map-gl';

import 'mapbox-gl/dist/mapbox-gl.css';
import PathwayIconClustersLayer from "./PathwayMap/PathwayIconClustersLayer";
import { GeofenceKind, KINDS, GeofenceData } from "./PathwayMap/PathwayIconTypes"
import ToggleButton from '@mui/material/ToggleButton';
import ToggleButtonGroup from '@mui/material/ToggleButtonGroup';
import DateTimeSlider from "./DateTimeSlider";
import { LineLayer, ScatterplotLayer } from "deck.gl/typed";
import TwitterTable from "./Table";
import FormGroup from '@mui/material/FormGroup';
import FormControlLabel from '@mui/material/FormControlLabel';
import Switch from '@mui/material/Switch';
import Slider from '@mui/material/Slider';
import Box from '@mui/material/Box';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import moment from 'moment-timezone';
import { fetchImpactData, GroupedTweetData, fetchStats, Stats } from "./api";
import { Backdrop, CircularProgress } from "@mui/material";
import { isEmpty } from "lodash";

const MAPBOX_ACCESS_TOKEN = process.env.MAPBOX_TOKEN

const theme = createTheme({
    palette: {
        primary: {
            main: '#0808F9',
        },
        secondary: {
            main: '#ff4f00',
        },
    },
});

function DeckGLOverlay(props: any): any {
    const overlay = useControl(() => new MapboxOverlay(props));
    overlay.setProps(props);
    return null;
}
export default function App() {
    const [totalTweets, setTotalTweets] = useState(0)
    const [rawData, setRawData] = useState<GroupedTweetData[]>([])
    const [showNumbers, setShowNumbers] = useState(true)
    const [sizeFactor, setSizeFactor] = useState(0.7)
    const [tooltipAdvanced, setTooltipAdvanced] = useState(false)

    const [selectedAuthor, setSelectedAuthor] = useState()
    const [selectedLocation, setSelectedLocation] = useState()
    const [timestamp, setTimestamp] = useState([0, 0])
    const [referencedData, setReferencedData] = useState([])
    const [sizeIndicator, setSizeIndicator] = useState('responses')

    const timeZone = useMemo(() => {
        // return 'America/Los_Angeles'
        // return 'UTC'
        return moment.tz.guess()
    }, [])

    const fetchReferences = async (author_username: string, timestamp: number[]) => {
        // const response = await fetch(`${BACKEND_HOST}:${BACKEND_PORT}/referenced?author_username=${author_username}&start=${timestamp[0]}&end=${timestamp[1]}`)
        // const fetchedData = await response.json()
        // setReferencedData(fetchedData.data)
    }

    const updateImpactData = async (start: number, end: number) => {
        const data = await fetchImpactData(start, end)
        setRawData(data.data)
    }

    const impactData = useMemo(() => {
        // const maxResponses = fetchedData.data.map((row: any) => row.responses_count).reduce((agg: number, cur: number) => Math.max(agg, cur), 1)
        const sizeFactor = 0.7
        const totalTweets = rawData.map((row) => row.responses_count).reduce((agg: number, cur: number) => agg + cur, 0)
        setTotalTweets(totalTweets)
        const processedData = rawData.flatMap((row, idx: number) => {
            const importanceVal = sizeIndicator === 'responses' ? row.total_responses : row.total_magic_influence
            const sizeVal = sizeIndicator === 'responses' ? row.responses_count : row.magic_influence
            const common = {
                ...row,
                rowId: idx,
                author_username: row.author_username,
                width: 1,
                coordinates: row.coord_shifted.slice(1, -1).split(',').map((x: any) => Number(x)),
                kind: row.mean_sentiment > 0 ? GeofenceKind.positive : row.mean_sentiment == 0 ? GeofenceKind.neutral : GeofenceKind.negative,
                importance: importanceVal,
                name: "TEST",
                confidence: "certain",
                visible: true,
                size: sizeVal,
                tooltip: {
                    html: tooltipAdvanced ? `
                <b>author:</b> ${row.author_username}<br />
                <b>location:</b> ${row.author_location}<br /><br />
                <b>responses(retweets and replies):</b> ${row.responses_count}<br />
                <ul>
                <li><b>close range:</b> ${row.close_count}</li>
                <li><b>medium range:</b> ${row.medium_count}</li>
                <li><b>far away:</b> ${row.far_count}</li>
                </ul>
                <b>Influence:</b> ${row.magic_influence}<br />
                <b>mean_sentiment:</b> ${row.mean_sentiment}
                `:
                        `
                <b>author:</b> ${row.author_username}<br />
                <b>location:</b> ${row.author_location}<br />
                <b>${row.responses_count}</b> retweets & replies in last ${(timestamp[1] - timestamp[0]) / 60} minutes <br />
                <b>${row.mean_sentiment > 0 ? 'POSITIVE' : row.mean_sentiment == 0 ? 'NEUTRAL' : 'NEGATIVE'}</b> reply sentiment (${row.mean_sentiment.toFixed(5)}) <br />
                ${row.magic_influence > 2 * row.responses_count ? "<b>Influence:</b> likely to be trending soon <br />" : ''}
                `
                }
            }
            return [
                { ...common, visible: common.visible && common.kind === 'positive', 'kind': GeofenceKind.positive },
                { ...common, visible: common.visible && common.kind === 'neutral', 'kind': GeofenceKind.neutral },
                { ...common, visible: common.visible && common.kind === 'negative', 'kind': GeofenceKind.negative }
            ]
        }).map((row: GeofenceData & GroupedTweetData) => (
            {
                ...row,
                coordinates: [...row.coordinates, KINDS[row.kind].zIndex * 10],
            }
        ))
        return processedData
    }, [rawData, sizeIndicator, tooltipAdvanced, timestamp])


    useEffect(() => {
        if (selectedAuthor) {
            fetchReferences(selectedAuthor, timestamp)
        } else {
            setReferencedData([])
        }
    }, [selectedAuthor, timestamp])

    const [stats, setStats] = useState<Stats | undefined>()

    const updateStats = async () => {
        const stats = await fetchStats()
        setStats(stats)
    }

    useEffect(() => {
        const interval = setInterval(() => {
            updateStats();
        }, 1000);
        return () => clearInterval(interval);
    }, []);

    const dataByKind = useMemo(() => {
        return Object.values(GeofenceKind).sort((a, b) => KINDS[a].zIndex - KINDS[b].zIndex).map(kind => {
            return {
                kind: kind,
                objects: impactData.filter((d: any) => d.kind === kind),
            }
        })
    }, [impactData])

    const layers = dataByKind.map(({ kind, objects }) => new PathwayIconClustersLayer({
        id: `clusteredicons-${kind}`,
        data: objects,
        showNumbers: showNumbers,
        sizeFactor: sizeFactor,
        staticSelectedIndices: new Set(),
        highlightedIndices: new Set(),
        selectedIndices: new Set(objects.filter((o) => JSON.stringify(o.coordinates.slice(0, 2)) == JSON.stringify(selectedLocation)).map(d => d.rowId)),
    }))

    const referencedLayer = new ScatterplotLayer({
        id: 'referenced',
        data: referencedData,
        getPosition: (d: any) => d.coord_from,
        radiusMinPixels: 2,
        radiusMaxPixels: 2,
    })
    const referencedLayerLines = new LineLayer({
        id: 'referenced-arcs',
        data: referencedData,
        getSourcePosition: (d: any) => d.coord_from,
        getTargetPosition: (d: any) => d.coord_to_shifted,
        getColor: [100, 100, 100, 200],
        widthMinPixels: 1,
        widthMaxPixels: 1,
        opacity: 0.01
    })

    const sliderChange = (start: number, end: number) => {
        setTimestamp([start, end])
        updateImpactData(start, end)
    }

    const handleSizeIndicatorChange = (
        event: React.MouseEvent<HTMLElement>,
        newSizeIndicator: string,
    ) => {
        if (newSizeIndicator !== null) {
            setSizeIndicator(newSizeIndicator)
        }
    };

    const [autoUpdateSlider, setAutoUpdateSlider] = useState(true)

    return <ThemeProvider theme={theme}>
        <div>
            <Backdrop
                sx={{ color: '#fff', zIndex: (theme) => theme.zIndex.drawer + 1 }}
                open={stats?.total_tweets == undefined}
            >
                <CircularProgress color="inherit" />
            </Backdrop>

            <h2>Twitter showcase</h2>
            <p>Legend:
                <span style={{ backgroundColor: `rgba(${KINDS['positive'].color}`, marginLeft: 10 }}>positive sentiment</span>
                <span style={{ backgroundColor: `rgba(${KINDS['neutral'].color}`, marginLeft: 10 }}>neutral sentiment</span>
                <span style={{ backgroundColor: `rgba(${KINDS['negative'].color}`, marginLeft: 10, marginRight: 50 }}>negative sentiment</span>
                Text on a circle represents the number of accounts in this area (zoom in to explore). Hover on a circle to get more information.
            </p>
            <p>Time zone: {timeZone}</p>
            <div style={{ display: 'flex' }}>
                <div style={{ height: 400, width: '50%', padding: 10, position: 'relative' }}>
                    <Map
                        initialViewState={{
                            latitude: 30,
                            longitude: 0,
                            zoom: 2
                        }}
                        mapStyle="mapbox://styles/mapbox/light-v9"
                        mapboxAccessToken={MAPBOX_ACCESS_TOKEN}
                    >
                        <DeckGLOverlay
                            layers={[referencedLayerLines, referencedLayer, ...layers]}
                            getTooltip={({ object }: any) => object?.tooltip}
                            onHover={(info: any, event: any) => { setSelectedAuthor(info?.object?.author_username) }}
                            onClick={(info: any, event: any) => { setSelectedLocation(info?.object?.coord_to) }}
                        />
                        <NavigationControl />
                    </Map>
                    <div className='timeonmap'>{moment.unix(timestamp[1]).tz(timeZone).format("YYYY-MM-DD, h:mm:ss a")}</div>
                </div>
                <div style={{ height: 400, width: '50%', padding: 10 }}>
                    <TwitterTable filterLocation={selectedLocation} data={impactData.filter(row => row.kind == 'neutral')} />  {/* this filter here is a hack, it does not filter rows*/}
                </div>
            </div>
            <h3>Good pairs of tweets in selected time: {totalTweets}</h3>
            <DateTimeSlider autoUpdateSlider={autoUpdateSlider} timeZone={timeZone} onChangeCallback={sliderChange} time_min={stats?.min_time_bucket ?? 0} time_max={(stats?.max_time_bucket ?? 0) + 59} />
            <h3>Total fetched tweets: {stats?.total_tweets}</h3>
            <h3>Total good pairs of tweets: {stats?.total_tweetpairs_good}</h3>
            <h3>Viz options:</h3>
            Size indicator:
            <ToggleButtonGroup
                color="primary"
                value={sizeIndicator}
                exclusive
                onChange={handleSizeIndicatorChange}
                aria-label="Platform"
                size="small"
            >
                <ToggleButton value="responses">Responses</ToggleButton>
                <ToggleButton value="influence">Influence</ToggleButton>
            </ToggleButtonGroup>
            <br />
            Size factor:
            <Box sx={{ width: 300 }}>
                <Slider
                    value={sizeFactor}
                    onChange={(e, newVal) => setSizeFactor(newVal as number)}
                    min={0.1}
                    max={4}
                    marks={true}
                    step={0.1}
                    valueLabelDisplay="auto"
                />
            </Box>
            <br />
            <FormGroup>
                <FormControlLabel control={<Switch checked={showNumbers} onChange={() => setShowNumbers(!showNumbers)} defaultChecked />} label="Show numbers" />
            </FormGroup>
            <FormGroup>
                <FormControlLabel control={<Switch checked={tooltipAdvanced} onChange={() => setTooltipAdvanced(!tooltipAdvanced)} defaultChecked />} label="Advanced tooltip" />
            </FormGroup>
            <FormGroup>
                <FormControlLabel control={<Switch checked={autoUpdateSlider} onChange={() => setAutoUpdateSlider(!autoUpdateSlider)} defaultChecked />} label="Auto update slider (streaming)" />
            </FormGroup>

        </div>
    </ThemeProvider>
}
