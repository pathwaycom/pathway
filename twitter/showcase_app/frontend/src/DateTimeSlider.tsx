import React, { useCallback, useEffect } from "react";
import { styled } from '@mui/material/styles';
import Box from '@mui/material/Box';
import Grid from '@mui/material/Grid';
import Typography from '@mui/material/Typography';
import Slider from '@mui/material/Slider';
import MuiInput from '@mui/material/Input';
import moment from 'moment-timezone';
import useThrottle from "./useThrottle";
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import Select, { SelectChangeEvent } from '@mui/material/Select';


const Input = styled(MuiInput)`
  width: 42px;
`;

interface DateTimeSliderProps {
    onChangeCallback: (start: number, end: number) => void
    time_min?: number,
    time_max?: number
}

export default function DateTimeSlider({ onChangeCallback, time_min, time_max }: DateTimeSliderProps) {
    const [timeWindow, setTimeWindow] = React.useState<number>(30 * 60)
    const [value, setValue] = React.useState<number[]>(
        [time_min, time_min + timeWindow],
    );
    const handleSliderChange = useCallback((
        event: Event,
        newValue: number | number[],
        activeThumb: number) => {
        if (!Array.isArray(newValue)) {
            return;
        }
        if (activeThumb === 0) {
            const clamped = Math.min(newValue[0], time_max - timeWindow);
            setValue([clamped, clamped + timeWindow]);
        } else {
            const clamped = Math.max(newValue[1], timeWindow);
            setValue([clamped - timeWindow, clamped]);
        }
    }, [timeWindow]);

    const throttledValue = useThrottle<number[]>(value, 200)

    useEffect(() => {
        onChangeCallback(throttledValue[0], throttledValue[1])
    }, [throttledValue])

    const valuetext = (value: number) => {
        return moment.unix(value).tz('America/Los_Angeles').format("YYYY-MM-DD, h:mm:ss a")
    }

    return (
        <Box style={{ margin: "0 70px" }}>
            <Typography id="input-slider" gutterBottom>
                Date
            </Typography>
            <Grid container spacing={2} alignItems="center">
                <Grid item xs>
                    <Slider
                        value={value}
                        onChange={handleSliderChange}
                        aria-labelledby="input-slider"
                        min={time_min}
                        max={time_max}
                        marks={true}
                        step={60 * 1}
                        valueLabelFormat={valuetext}
                        valueLabelDisplay="on"
                        disableSwap
                    />
                </Grid>
                <Grid item>
                    <Box sx={{ minWidth: 150 }}>
                        <FormControl fullWidth>
                            <InputLabel id="demo-simple-select-label">Time window (mins):</InputLabel>
                            <Select
                                labelId="demo-simple-select-label"
                                id="demo-simple-select"
                                value={timeWindow / 60}
                                label="Time window (mins):"
                                onChange={(event) => { setTimeWindow(Number(event.target.value) * 60); setValue([value[0], value[0] + Number(event.target.value) * 60]) }}
                            >
                                <MenuItem value={5}>5</MenuItem>
                                <MenuItem value={10}>10</MenuItem>
                                <MenuItem value={15}>15</MenuItem>
                                <MenuItem value={30}>30</MenuItem>
                                <MenuItem value={60}>60</MenuItem>
                                <MenuItem value={120}>120</MenuItem>
                                <MenuItem value={60 * 100}>All</MenuItem>
                            </Select>
                        </FormControl>
                    </Box>
                </Grid>
            </Grid>
        </Box>
    );
}