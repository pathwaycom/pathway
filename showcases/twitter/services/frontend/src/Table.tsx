import * as React from 'react';
import { DataGrid, GridColDef, GridToolbar } from '@mui/x-data-grid';
import { Link } from '@mui/material';
import { GroupedTweetData } from './api';
import { GeofenceData } from './PathwayMap/PathwayIconTypes';

interface TableProps {
    data: (GroupedTweetData & GeofenceData)[],
    filterLocation: any
}

const coordOperator = [
    {
        label: 'Coord_to',
        value: 'coord_to',
        getApplyFilterFn: (filterItem: any) => {
            if (
                !filterItem.columnField ||
                !filterItem.value ||
                !filterItem.operatorValue
            ) {
                return null;
            }

            return (params: any) => {
                return JSON.stringify(params.value) == JSON.stringify(filterItem.value);
            };
        },
        //   InputComponent: RatingInputValue,
        //   InputComponentProps: { type: 'number' },
    },
];

const columns: GridColDef[] = [
    { field: 'author_username', headerName: 'Username', width: 100, renderCell: (params: any) => <Link target="_blank" href={`http://twitter.com/${params.value}`}>{params.value}</Link> },
    { field: 'author_location', headerName: 'Location', width: 140 },
    { field: 'responses_count', headerName: 'Responses', type: 'number', width: 150 },
    { field: 'magic_influence', headerName: 'Influence', type: 'number', width: 120 },
    { field: 'mean_sentiment', headerName: 'Mean sentiment', type: 'number', width: 120 },
    { field: 'coord_to', headerName: 'LON, LAT', width: 200, filterOperators: coordOperator },
];

export default function TwitterTable({ data, filterLocation }: TableProps) {
    return (
        <DataGrid
            initialState={{
                sorting: {
                    sortModel: [{ field: 'responses_count', sort: 'desc' }],
                },
            }}
            rows={data}
            columns={columns}
            pageSize={10}
            rowsPerPageOptions={[5]}
            // components={{
            //     Toolbar: GridToolbar,
            // }}
            getRowId={(row: any) => row.author_username}
            filterModel={{
                items: filterLocation ? [{ columnField: 'coord_to', operatorValue: 'coord_to', value: filterLocation }] : [],
            }}
        // onFilterModelChange={(newFilterModel) => setFilterModel(newFilterModel)}
        />
    );
}