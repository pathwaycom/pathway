const API_HOST = 'http://localhost' //process.env.API_HOST
const API_PORT = process.env.API_PORT


export interface RawData {
    data: GroupedTweetData[]
}

export interface GroupedTweetData {
    tweet_to_author_id: string,
    author_username: string,
    author_location: string,
    coord_to: string,
    coord_shifted: string,
    total_responses: number,
    total_magic_influence: number,
    responses_count: number,
    mean_sentiment: number,
    magic_influence: number,
    far_count: number,
    medium_count: number,
    close_count: number,
}


export interface Stats {
    min_time_bucket: number,
    max_time_bucket: number
}

export async function fetchImpactData(start: number, end: number): Promise<RawData> {
    const response = await fetch(`${API_HOST}:${API_PORT}/impact?start=${start}&end=${end}`)
    return await response.json()
}

export async function fetchStats(): Promise<Stats> {
    const response = await fetch(`${API_HOST}:${API_PORT}/stats`)
    return await response.json()
}

// export async function fetchReferences(author_username: string, timestamp: number): Promise<GroupedTweetData[]> {
//     const response = await fetch(`${BACKEND_HOST}:${BACKEND_PORT}/referenced?author_username=${author_username}&start=${timestamp[0]}&end=${timestamp[1]}`)
//     return await response.json()
// }