type RGBAColor = [number, number, number, number];
import { values } from "lodash";
import "../assets/circle.png"

export const NavalColors = {
    zero: "#3A748A",
    one: "#4B9A95",
    two: "#5EAB8B",
    three: "#73BC84",
    four: "#92CC8B",
    five: "#BEDDA5",
    fancy: "#35EEC1",
    six: "#EBD198",
};

// The following colors will be used if you pick "Automatic" color
export const BaseColors = {
    Blue: "#356AFF",
    Red: "#E92828",
    Green: "#3BD973",
    Purple: "#604FE9",
    Cyan: "#50F5ED",
    Orange: "#FB8D3D",
    "Light Blue": "#799CFF",
    Lilac: "#B554FF",
    "Light Green": "#8CFFB4",
    Brown: "#A55F2A",
    Black: "#000000",
    Gray: "#494949",
    Pink: "#FF7DE3",
    "Dark Blue": "#002FB4",
};

// Additional colors for the user to choose from
export const AdditionalColors = {
    "Indian Red": "#981717",
    "Green 2": "#17BF51",
    "Green 3": "#049235",
    "Dark Turquoise": "#00B6EB",
    "Dark Violet": "#A58AFF",
    "Pink 2": "#C63FA9",
};

export const ColorPaletteArray = values(NavalColors);

export function hexToRgb(hex: string) {
    if (hex == null) {
        return [255, 255, 255]
    }
    return hex.match(/\w\w/g).map(x => parseInt(x, 16));
}

export function hexToRgba(hex: string, alpha = 1) {
    const [r, g, b] = hexToRgb(hex)
    return `rgba(${r},${g},${b},${alpha})`;
}

export function hexToArray(hex: string, alpha: number = 255) {
    const [r, g, b] = hexToRgb(hex)
    return [r, g, b, alpha]
}

export function getIndexColor(index: number) {
    const n = ColorPaletteArray.length;
    return ColorPaletteArray[((index % n) + n) % n];
}


export interface ObjectRow {
    rowId: number,
    visible: boolean,
    tooltip: any,
}

export enum GeofenceKind {
    positive = "positive",
    negative = "negative",
    neutral = "neutral"
}

interface IconProps {
    icon: { name: string, anchorY: number },
    textPixelsOffset: [number, number],
    color: RGBAColor,
    zIndex: number,  // larger = closer to camera
    sizeMultiplier: number
}

export const KINDS: Record<GeofenceKind, IconProps> = {
    neutral: {
        icon: { name: 'circle', anchorY: 64 },
        textPixelsOffset: [0, 0],
        color: hexToArray('#00FFFF', 200) as RGBAColor, //dark green
        zIndex: 1,
        sizeMultiplier: 4,
    },
    negative: {
        icon: { name: 'circle', anchorY: 64 },
        textPixelsOffset: [0, 0],
        color: hexToArray('#FF00FF', 200) as RGBAColor, //dark green
        zIndex: 5,
        sizeMultiplier: 4,
    },
    positive: {
        icon: { name: 'circle', anchorY: 64 },
        textPixelsOffset: [0, 0],
        color: hexToArray('#08FF08', 200) as RGBAColor, //dark green
        zIndex: 10,
        sizeMultiplier: 4,
    },

}

export interface GeofenceData extends ObjectRow {
    rowId: number
    width: number,
    coordinates: any,
    kind: GeofenceKind,
    importance: number,
    name: string,
    confidence: string,
    size: number
}

export interface GeofenceIconData extends GeofenceData {
    size: number,
    text: string,
    visible: boolean
}
