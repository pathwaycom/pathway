extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::*;

fn main() {

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        // An input for (x,y,z) placements.
        let mut xyzs = InputSession::<_,_,isize>::new();

        // Inputs for (x,y) and (x,z) goals.
        let mut xy_goal = InputSession::new();
        let mut xz_goal = InputSession::new();

        let mut probe = Handle::new();

        // Dataflow to validate input against goals.
        worker.dataflow(|scope| {

            // Introduce inputs to the scope.
            let xyzs = xyzs.to_collection(scope);
            let xy_goal = xy_goal.to_collection(scope);
            let xz_goal = xz_goal.to_collection(scope);

            // Report unmet XY goals, and met XY non-goals.
            let xy_errors =
            xyzs.map(|(x,y,_)| (x,y))
                .distinct()
                .negate()
                .concat(&xy_goal)
                .consolidate();

            // Report unmet XZ goals, and met XZ non-goals.
            let xz_errors =
            xyzs.map(|(x,_,z)| (x,z))
                .distinct()
                .negate()
                .concat(&xz_goal)
                .consolidate();

            let xy_total = xy_errors.distinct().map(|_| ());
            let xz_total = xz_errors.distinct().map(|_| ());
            xy_total
                .concat(&xz_total)
                .distinct()
                .inspect(|x| println!("Not done: {:?}", x))
                .probe_with(&mut probe);
        });

        // Dataflow to produce maximum inputs.
        worker.dataflow(|scope| {

            // Introduce goals to the scope.
            let xy_goal = xy_goal.to_collection(scope);
            let xz_goal = xz_goal.to_collection(scope);

            let xy_xs = xy_goal.map(|(x,_)| (x,()));
            let xz_xs = xz_goal.map(|(x,_)| (x,()));
            xy_xs.join(&xz_xs)
                 .map(|_| ())
                 .consolidate()
                 .inspect(|x| println!("Maximum solution size: {}", x.2))
                 .probe_with(&mut probe);

            // // For each x, produce valid pairs of y and z.
            // xy_goal
            //     .join(&xz_goal)
            //     .map(|(x,(y,z))| (x,y,z))
            //     .inspect(|x| println!("Maximum solution: {:?}", x))
            //     .probe_with(&mut probe);
        });


        // Dataflow to produce minimum inputs.
        worker.dataflow(|scope| {

            // Introduce goals to the scope.
            let xy_goal = xy_goal.to_collection(scope);
            let xz_goal = xz_goal.to_collection(scope);

            let xy_xs = xy_goal.map(|(x,_)| x).count();
            let xz_xs = xz_goal.map(|(x,_)| x).count();
            xy_xs.join(&xz_xs)
                 .explode(|(_,(ys,zs))| Some(((), ::std::cmp::max(ys,zs))))
                 .consolidate()
                 .inspect(|x| println!("Minimum solution size: {}", x.2))
                 .probe_with(&mut probe);

            // // Produce pairs (x, ys) and (x, zs).
            // let xy_xs = xy_goal.group(|_x,ys,out|
            //     out.push((ys.iter().map(|(&y,_)| y).collect::<Vec<_>>(), 1))
            // );
            // let xz_xs = xz_goal.group(|_x,zs,out|
            //     out.push((zs.iter().map(|(&z,_)| z).collect::<Vec<_>>(), 1))
            // );

            // xy_xs.join(&xz_xs)
            //      .flat_map(|(x,(ys, zs))| {
            //         let max = ::std::cmp::max(ys.len(), zs.len());
            //         let ys = ys.into_iter().cycle();
            //         let zs = zs.into_iter().cycle();
            //         ys.zip(zs).take(max).map(move |(y,z)| (x,y,z))
            //      })
            //     .inspect(|x| println!("Minimum solution: {:?}", x))
            //     .probe_with(&mut probe);

        });

        // Introduce XY projektion.
        xy_goal.insert((0, 0));
        xy_goal.insert((0, 1));
        xy_goal.insert((0, 3));
        xy_goal.insert((0, 4));
        xy_goal.insert((1, 1));
        xy_goal.insert((1, 3));
        xy_goal.insert((2, 1));
        xy_goal.insert((2, 2));
        xy_goal.insert((3, 2));
        xy_goal.insert((3, 3));
        xy_goal.insert((3, 4));
        xy_goal.insert((4, 0));
        xy_goal.insert((4, 1));
        xy_goal.insert((4, 2));

        // Introduce XZ projektion.
        xz_goal.insert((0, 2));
        xz_goal.insert((0, 3));
        xz_goal.insert((0, 4));
        xz_goal.insert((1, 2));
        xz_goal.insert((1, 4));
        xz_goal.insert((2, 1));
        xz_goal.insert((2, 2));
        xz_goal.insert((2, 3));
        xz_goal.insert((3, 0));
        xz_goal.insert((3, 1));
        xz_goal.insert((3, 3));
        xz_goal.insert((3, 4));
        xz_goal.insert((4, 1));
        xz_goal.insert((4, 4));

        // Advance one round.
        xyzs.advance_to(1); xyzs.flush();
        xy_goal.advance_to(1); xy_goal.flush();
        xz_goal.advance_to(1); xz_goal.flush();

        // Introduce candidate solution.
        xyzs.insert((0, 0, 2));
        xyzs.insert((0, 1, 3));
        xyzs.insert((0, 3, 4));
        xyzs.insert((0, 4, 4));
        xyzs.insert((1, 1, 2));
        xyzs.insert((1, 3, 4));
        xyzs.insert((2, 1, 1));
        xyzs.insert((2, 2, 2));
        xyzs.insert((2, 2, 3));
        xyzs.insert((3, 2, 0));
        xyzs.insert((3, 3, 1));
        xyzs.insert((3, 4, 3));
        xyzs.insert((3, 4, 4));
        xyzs.insert((4, 0, 1));
        xyzs.insert((4, 1, 4));
        xyzs.insert((4, 2, 4));

        // Advance another round.
        xyzs.advance_to(2); xyzs.flush();
        xy_goal.advance_to(2); xy_goal.flush();
        xz_goal.advance_to(2); xz_goal.flush();

    }).unwrap();
}