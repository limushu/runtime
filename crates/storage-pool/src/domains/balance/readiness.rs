//! Read-only placement checks, shared by admission and migration continuation.
use crate::{DataState, DiskId, MemberDisk, VirtualDisk, support::reject};
use std::collections::BTreeSet;
use workflow_foundation::{Outcome, Readiness};

pub(crate) fn evacuation_readiness(
    disks: &[MemberDisk],
    volumes: &[VirtualDisk],
    targets: &[DiskId],
) -> Outcome<Readiness> {
    if volumes
        .iter()
        .flat_map(|v| &v.groups)
        .flat_map(|g| &g.entries)
        .any(|e| e.data != DataState::Valid)
    {
        return Ok(Readiness::Blocked("waiting for healthy BG replicas".into()));
    }
    // Re-evaluate committed placement/capacity on initial entry and every resume.
    for tier in disks
        .iter()
        .filter(|d| targets.contains(&d.id))
        .map(|d| &d.tier)
        .collect::<BTreeSet<_>>()
    {
        let needed: usize = disks
            .iter()
            .filter(|d| &d.tier == tier && targets.contains(&d.id))
            .map(|d| d.allocated.len())
            .sum();
        let free: usize = disks
            .iter()
            .filter(|d| &d.tier == tier && !targets.contains(&d.id) && d.allocatable())
            .map(|d| (d.capacity_blks as usize).saturating_sub(d.allocated.len()))
            .sum();
        if free < needed {
            return Ok(Readiness::Blocked(
                "insufficient destination capacity".into(),
            ));
        }
    }
    for group in volumes.iter().flat_map(|v| &v.groups) {
        let moving: Vec<_> = group
            .entries
            .iter()
            .filter(|e| targets.contains(&e.disk))
            .collect();
        if moving.is_empty() {
            continue;
        }
        let source = disks
            .iter()
            .find(|d| d.id == moving[0].disk)
            .ok_or_else(|| reject("missing BG source"))?;
        let occupied: BTreeSet<_> = group
            .entries
            .iter()
            .filter(|e| !targets.contains(&e.disk))
            .filter_map(|e| disks.iter().find(|d| d.id == e.disk).map(|d| &d.node))
            .collect();
        let candidates: BTreeSet<_> = disks
            .iter()
            .filter(|d| {
                d.tier == source.tier
                    && !targets.contains(&d.id)
                    && d.allocatable()
                    && d.allocated.len() < d.capacity_blks as usize
                    && !occupied.contains(&d.node)
            })
            .map(|d| &d.node)
            .collect();
        if candidates.len() < moving.len() {
            return Ok(Readiness::Blocked(
                "insufficient destination failure domains".into(),
            ));
        }
    }
    Ok(Readiness::Ready)
}
