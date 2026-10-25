use workflow_foundation::{TaskId, scheduling::*};
struct Policy;
impl SchedulingPolicy<u8> for Policy {
    fn on_task(&self, old: &u8, new: &u8) -> Conflict {
        Conflict {
            decision: if old & new == 0 {
                ConflictDecision::Independent
            } else {
                ConflictDecision::CancelThenWait
            },
            reason: "same-object",
        }
    }
}
fn ids() -> [TaskId; 4] {
    std::array::from_fn(|i| format!("1/{:032x}/queue/{}", 1, i + 1).parse().unwrap())
}
fn candidate(id: TaskId, descriptor: &u8, started: bool, accepted: bool) -> Candidate<'_, u8> {
    Candidate {
        id,
        descriptor,
        started,
        accepted,
        paused: false,
    }
}
#[test]
fn only_head_can_cancel_current_and_queued_middle_is_never_a_cancel_target() {
    let [a, b, c, _] = ids();
    let plan = plan(
        &Policy,
        &[
            candidate(a, &1, true, true),
            candidate(b, &1, false, true),
            candidate(c, &1, false, true),
        ],
    );
    assert_eq!(plan.cancellations, vec![(b, a)]);
    assert_eq!(plan.blockers[&c], vec![b]);
    assert!(plan.ready.is_empty());
    let plan = workflow_foundation::scheduling::plan(
        &Policy,
        &[candidate(b, &1, true, true), candidate(c, &1, false, true)],
    );
    assert_eq!(plan.cancellations, vec![(c, b)]);
}
#[test]
fn pending_durable_registration_reserves_its_queue_place_without_control_effects() {
    let [a, b, c, _] = ids();
    let plan = plan(
        &Policy,
        &[
            candidate(a, &1, true, true),
            candidate(b, &1, false, false),
            candidate(c, &1, false, true),
        ],
    );
    assert!(plan.cancellations.is_empty());
    assert_eq!(plan.blockers[&c], vec![b]);
    assert!(plan.ready.is_empty());
}
#[test]
fn multi_object_waiter_cannot_cancel_another_current_before_becoming_head_everywhere() {
    let [a, d, b, c] = ids();
    // A owns X, D owns Y. B waits on X. C needs X+Y and is behind B.
    let plan = plan(
        &Policy,
        &[
            candidate(a, &1, true, true),
            candidate(d, &2, true, true),
            candidate(b, &1, false, true),
            candidate(c, &3, false, true),
        ],
    );
    assert_eq!(plan.cancellations, vec![(b, a)]);
    assert_eq!(plan.blockers[&c], vec![d, b]);
}
#[test]
fn disjoint_task_runs_while_another_object_has_a_queue() {
    let [a, b, c, d] = ids();
    let plan = plan(
        &Policy,
        &[
            candidate(a, &1, true, true),
            candidate(b, &1, false, true),
            candidate(c, &1, false, true),
            candidate(d, &2, false, true),
        ],
    );
    assert_eq!(plan.ready, vec![d]);
    assert_eq!(plan.cancellations, vec![(b, a)]);
}
