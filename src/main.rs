use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::CountTotal;
use differential_dataflow::trace::TraceReader;
use timely::dataflow::operators::Probe;
use timely::dataflow::ProbeHandle;
use timely::progress::frontier::AntichainRef;

fn main() {
    timely::execute_directly(move |worker| {
        let mut probe = ProbeHandle::new();
        let (mut input, mut trace) = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection();
            let arrange = input.arrange_by_self();
            arrange.stream.probe_with(&mut probe);
            (handle, arrange.trace)
        });

        // ingest some batches
        for _ in 0..10 {
            input.insert(10);
            input.advance_to(input.time() + 1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));
        }

        // advance the trace
        trace.set_logical_compaction(AntichainRef::new(&[2]));
        trace.set_physical_compaction(AntichainRef::new(&[2]));

        worker.dataflow::<u32, _, _>(|scope| {
            let arrange = trace.import(scope);
            arrange
                .count_total() // <-- panic
                .inspect(|d| println!("{:?}", d));
        });
    })
}
