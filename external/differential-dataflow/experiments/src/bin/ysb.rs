extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::input::Input;
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Consolidate;

#[derive(Clone)]
pub enum AdType {
    Banner,
    Modal,
    SponsoredSearch,
    Mail,
    Mobile,
}

#[derive(Clone, Eq, PartialEq)]
pub enum EventType {
    View,
    Click,
    Purchase,
}

#[derive(Clone)]
pub struct View {
    pub user_id: usize,
    pub page_id: usize,
    pub ad_id: usize,
    pub ad_type: AdType,
    pub event_type: EventType,
    pub event_time: usize,
    pub ip_address: [u8;4],
}

impl View {
    pub fn rand_from<R: Rng>(rng: &mut R, ad_ids: &[usize]) -> Self {
        View {
            user_id: rng.gen(),
            page_id: rng.gen(),
            ad_id: *rng.choose(ad_ids).unwrap(),
            ad_type: rng.choose(&[
                AdType::Banner,
                AdType::Modal,
                AdType::SponsoredSearch,
                AdType::Mail,
                AdType::Mobile,]).unwrap().clone(),
            event_type: rng.choose(&[
                EventType::View,
                EventType::Click,
                EventType::Purchase]).unwrap().clone(),
            event_time: rng.gen(),
            ip_address: rng.gen(),
        }
    }
}

fn main() {

    let campaigns: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let ads_per: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();
    let inspect: bool = std::env::args().find(|x| x == "inspect").is_some();

    timely::execute_from_args(std::env::args().skip(4), move |worker| {

        let timer = ::std::time::Instant::now();

        let index = worker.index();
        let peers = worker.peers();

        // create a degree counting differential dataflow
        let (mut views, mut links, probe) = worker.dataflow(|scope| {

            // create edge input, count a few ways.
            let (views_input, views) = scope.new_collection();
            let (links_input, links) = scope.new_collection();

            let probe =
            links
                .semijoin(&views)
                .map(|(_ad, campaign)| campaign)
                .consolidate()
                .inspect(move |x| if inspect { println!("{:?}:\t{:?}", x.0, x.2); })
                .probe();

            (views_input, links_input, probe)
        });

        let seed_global: &[_] = &[1, 2, 3, peers];
        let mut rng_global: StdRng = SeedableRng::from_seed(seed_global);

        // generate ad and campaign identifiers.
        let mut ad_identifiers = Vec::new();
        for _ in 0 .. campaigns {
            let campaign_id: usize = rng_global.gen();
            for _ in 0 .. ads_per {
                let ad_id: usize = rng_global.gen();
                ad_identifiers.push(ad_id);
                if index == 0 {
                    links.insert((ad_id, campaign_id));
                }
            }
        }
        links.close();

        let seed_worker: &[_] = &[1, 2, 3, index];
        let mut rng_worker: StdRng = SeedableRng::from_seed(seed_worker);

        let mut typed_things = Vec::new();
        for _ in 0 .. (1 << 16) {
            typed_things.push(View::rand_from(&mut rng_worker, &ad_identifiers[..]));
        }

        let mut counter = 0;
        let mut next = 10;
        loop {

            let elapsed_s = timer.elapsed().as_secs();
            if elapsed_s >= next {
                views.advance_to(elapsed_s);
                views.flush();
                worker.step_while(|| probe.less_than(views.time()));
                next = next + 10;
                println!("latency: {:?}ns; rate: {:?}/s", timer.elapsed().subsec_nanos(), counter / 10);
                counter = 0;
            }

            for _ in 0 .. batch {
                let mut rand_idx: usize = rng_worker.gen();
                for _ in 0 .. 4 {
                    let rand_thing = &typed_things[rand_idx % (1 << 16)];
                    if rand_thing.event_type == EventType::Purchase {
                        views.insert(rand_thing.ad_id);
                    }
                    rand_idx = rand_idx >> 16;
                }
            }

            worker.step();
            counter += 4 * batch;
        }
    }).unwrap();
}