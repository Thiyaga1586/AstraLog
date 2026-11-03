use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, VecDeque},
    fmt,
};

use astralog_common::TopicId;
use astralog_protocol::api::{
    MAX_GROUP_MEMBERS, MAX_GROUP_PARTITIONS, MAX_MEMBER_ASSIGNMENT_PARTITIONS,
    MAX_SUBSCRIPTION_TOPICS, MemberId, TopicPartitionV0,
};

use crate::MemberRecordV1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssignorError {
    TooManyMembers,
    TooManyPartitions,
    TooManySubscriptions { member_id: MemberId },
    NonCanonicalSubscriptions { member_id: MemberId },
    MemberMapKeyMismatch { member_id: MemberId },
    NoEligibleOwner { topic_id: TopicId },
    CapacityExceeded,
    NoFeasibleCompletion,
    ArithmeticOverflow,
}

impl fmt::Display for AssignorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyMembers => f.write_str("group member count exceeds frozen P6 bound"),
            Self::TooManyPartitions => f.write_str("group partition count exceeds frozen P6 bound"),
            Self::TooManySubscriptions { member_id } => {
                write!(
                    f,
                    "member {member_id:?} subscription count exceeds frozen P6 bound"
                )
            }
            Self::NonCanonicalSubscriptions { member_id } => {
                write!(f, "member {member_id:?} subscriptions are not canonical")
            }
            Self::MemberMapKeyMismatch { member_id } => {
                write!(
                    f,
                    "member map key does not match member record {member_id:?}"
                )
            }
            Self::NoEligibleOwner { topic_id } => {
                write!(f, "topic {topic_id:?} has no eligible subscriber")
            }
            Self::CapacityExceeded => f.write_str("no assignment fits frozen per-member capacity"),
            Self::NoFeasibleCompletion => {
                f.write_str("no completion preserves the frozen assignor optimum")
            }
            Self::ArithmeticOverflow => f.write_str("assignor arithmetic overflow"),
        }
    }
}

impl std::error::Error for AssignorError {}

pub type AssignorResult<T> = core::result::Result<T, AssignorError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniformStickyAssignment {
    pub assignments: BTreeMap<TopicPartitionV0, MemberId>,
    pub max_load: usize,
    pub movement_count: usize,
}

pub fn uniform_sticky_assign(
    members: &BTreeMap<MemberId, MemberRecordV1>,
    current_assignments: &BTreeMap<TopicPartitionV0, MemberId>,
    topic_partition_counts: &BTreeMap<TopicId, u32>,
) -> AssignorResult<UniformStickyAssignment> {
    validate_input(members, current_assignments, topic_partition_counts)?;

    let total_partitions = total_partition_count(topic_partition_counts)?;
    if total_partitions == 0 {
        return Ok(UniformStickyAssignment {
            assignments: BTreeMap::new(),
            max_load: 0,
            movement_count: 0,
        });
    }
    if members.is_empty() {
        return Err(AssignorError::NoFeasibleCompletion);
    }

    let eligibility = build_eligibility(members, topic_partition_counts)?;
    let min_max_load = minimum_feasible_max_load(&eligibility, topic_partition_counts, members)?;

    let same_subscription_floor = same_subscription_floor(members, total_partitions);
    let mut remaining_counts = topic_partition_counts.clone();
    let mut remaining_capacities = members
        .keys()
        .copied()
        .map(|member_id| {
            (
                member_id,
                u32::try_from(min_max_load).map_err(|_| AssignorError::ArithmeticOverflow),
            )
        })
        .map(|(member_id, capacity)| capacity.map(|capacity| (member_id, capacity)))
        .collect::<AssignorResult<BTreeMap<_, _>>>()?;
    let mut remaining_lower = members
        .keys()
        .copied()
        .map(|member_id| (member_id, same_subscription_floor))
        .collect::<BTreeMap<_, _>>();
    let mut remaining_current =
        build_eligible_current_counts(members, current_assignments, topic_partition_counts);

    let optimum_movement = minimum_remaining_movement(
        &eligibility,
        &remaining_counts,
        &remaining_current,
        &remaining_capacities,
        &remaining_lower,
    )?
    .ok_or(AssignorError::NoFeasibleCompletion)?;

    let mut fixed_movement = 0_usize;
    let mut assignments = BTreeMap::new();

    for (&topic_id, &partition_count) in topic_partition_counts {
        if partition_count == 0 {
            continue;
        }
        let candidates = eligibility
            .get(&topic_id)
            .ok_or(AssignorError::NoEligibleOwner { topic_id })?;
        for partition in 0..partition_count {
            let topic_partition = TopicPartitionV0 {
                topic_id,
                partition,
            };
            let eligible_current = current_assignments
                .get(&topic_partition)
                .copied()
                .filter(|member_id| member_is_eligible(members, *member_id, topic_id));

            let mut chosen = None;
            for &candidate in candidates {
                let capacity = *remaining_capacities
                    .get(&candidate)
                    .ok_or(AssignorError::NoFeasibleCompletion)?;
                if capacity == 0 {
                    continue;
                }

                let old_topic_count = *remaining_counts
                    .get(&topic_id)
                    .ok_or(AssignorError::NoFeasibleCompletion)?;
                let old_capacity = capacity;
                let old_lower = *remaining_lower
                    .get(&candidate)
                    .ok_or(AssignorError::NoFeasibleCompletion)?;
                let current_key = eligible_current.map(|owner| (topic_id, owner));
                let old_current_count = current_key
                    .and_then(|key| remaining_current.get(&key).copied())
                    .unwrap_or(0);

                remaining_counts.insert(topic_id, old_topic_count - 1);
                remaining_capacities.insert(candidate, old_capacity - 1);
                remaining_lower.insert(candidate, old_lower.saturating_sub(1));
                if let Some(key) = current_key {
                    if old_current_count == 0 {
                        return Err(AssignorError::NoFeasibleCompletion);
                    }
                    if old_current_count == 1 {
                        remaining_current.remove(&key);
                    } else {
                        remaining_current.insert(key, old_current_count - 1);
                    }
                }

                let movement_increment = usize::from(
                    eligible_current.is_some_and(|current_owner| current_owner != candidate),
                );
                let feasible = minimum_remaining_movement(
                    &eligibility,
                    &remaining_counts,
                    &remaining_current,
                    &remaining_capacities,
                    &remaining_lower,
                )?
                .is_some_and(|remaining| {
                    fixed_movement
                        .checked_add(movement_increment)
                        .and_then(|value| value.checked_add(remaining))
                        == Some(optimum_movement)
                });

                if feasible {
                    chosen = Some((candidate, movement_increment));
                    break;
                }

                remaining_counts.insert(topic_id, old_topic_count);
                remaining_capacities.insert(candidate, old_capacity);
                remaining_lower.insert(candidate, old_lower);
                if let Some(key) = current_key {
                    remaining_current.insert(key, old_current_count);
                }
            }

            let (member_id, movement_increment) =
                chosen.ok_or(AssignorError::NoFeasibleCompletion)?;
            assignments.insert(topic_partition, member_id);
            fixed_movement = fixed_movement
                .checked_add(movement_increment)
                .ok_or(AssignorError::ArithmeticOverflow)?;
        }
    }

    if fixed_movement != optimum_movement
        || remaining_lower.values().any(|required| *required != 0)
        || remaining_counts.values().any(|count| *count != 0)
    {
        return Err(AssignorError::NoFeasibleCompletion);
    }

    let actual_max_load = assignment_max_load(&assignments, members);
    if actual_max_load != min_max_load {
        return Err(AssignorError::NoFeasibleCompletion);
    }

    Ok(UniformStickyAssignment {
        assignments,
        max_load: min_max_load,
        movement_count: optimum_movement,
    })
}

fn validate_input(
    members: &BTreeMap<MemberId, MemberRecordV1>,
    current_assignments: &BTreeMap<TopicPartitionV0, MemberId>,
    topic_partition_counts: &BTreeMap<TopicId, u32>,
) -> AssignorResult<()> {
    if members.len() > MAX_GROUP_MEMBERS {
        return Err(AssignorError::TooManyMembers);
    }
    for (member_id, member) in members {
        if *member_id != member.member_id {
            return Err(AssignorError::MemberMapKeyMismatch {
                member_id: *member_id,
            });
        }
        if member.subscriptions.len() > MAX_SUBSCRIPTION_TOPICS {
            return Err(AssignorError::TooManySubscriptions {
                member_id: *member_id,
            });
        }
        if member
            .subscriptions
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(AssignorError::NonCanonicalSubscriptions {
                member_id: *member_id,
            });
        }
    }
    total_partition_count(topic_partition_counts)?;
    if current_assignments.len() > MAX_GROUP_PARTITIONS {
        return Err(AssignorError::TooManyPartitions);
    }
    Ok(())
}

fn total_partition_count(topic_partition_counts: &BTreeMap<TopicId, u32>) -> AssignorResult<usize> {
    let total = topic_partition_counts
        .values()
        .try_fold(0_usize, |total, count| {
            total
                .checked_add(
                    usize::try_from(*count).map_err(|_| AssignorError::ArithmeticOverflow)?,
                )
                .ok_or(AssignorError::ArithmeticOverflow)
        })?;
    if total > MAX_GROUP_PARTITIONS {
        return Err(AssignorError::TooManyPartitions);
    }
    Ok(total)
}

fn build_eligibility(
    members: &BTreeMap<MemberId, MemberRecordV1>,
    topic_partition_counts: &BTreeMap<TopicId, u32>,
) -> AssignorResult<BTreeMap<TopicId, Vec<MemberId>>> {
    let mut eligibility = BTreeMap::new();
    for (&topic_id, &count) in topic_partition_counts {
        if count == 0 {
            continue;
        }
        let eligible = members
            .iter()
            .filter_map(|(member_id, member)| {
                member
                    .subscriptions
                    .binary_search(&topic_id)
                    .is_ok()
                    .then_some(*member_id)
            })
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            return Err(AssignorError::NoEligibleOwner { topic_id });
        }
        eligibility.insert(topic_id, eligible);
    }
    Ok(eligibility)
}

fn member_is_eligible(
    members: &BTreeMap<MemberId, MemberRecordV1>,
    member_id: MemberId,
    topic_id: TopicId,
) -> bool {
    members
        .get(&member_id)
        .is_some_and(|member| member.subscriptions.binary_search(&topic_id).is_ok())
}

fn build_eligible_current_counts(
    members: &BTreeMap<MemberId, MemberRecordV1>,
    current_assignments: &BTreeMap<TopicPartitionV0, MemberId>,
    topic_partition_counts: &BTreeMap<TopicId, u32>,
) -> BTreeMap<(TopicId, MemberId), u32> {
    let mut counts = BTreeMap::new();
    for (topic_partition, member_id) in current_assignments {
        if topic_partition_counts
            .get(&topic_partition.topic_id)
            .is_some_and(|count| topic_partition.partition < *count)
            && member_is_eligible(members, *member_id, topic_partition.topic_id)
        {
            *counts
                .entry((topic_partition.topic_id, *member_id))
                .or_default() += 1;
        }
    }
    counts
}

fn same_subscription_floor(
    members: &BTreeMap<MemberId, MemberRecordV1>,
    total_partitions: usize,
) -> u32 {
    let mut iter = members.values();
    let Some(first) = iter.next() else {
        return 0;
    };
    if !iter.all(|member| member.subscriptions == first.subscriptions) {
        return 0;
    }
    u32::try_from(total_partitions / members.len()).unwrap_or(0)
}

fn minimum_feasible_max_load(
    eligibility: &BTreeMap<TopicId, Vec<MemberId>>,
    topic_partition_counts: &BTreeMap<TopicId, u32>,
    members: &BTreeMap<MemberId, MemberRecordV1>,
) -> AssignorResult<usize> {
    let total = total_partition_count(topic_partition_counts)?;
    let high = total.min(MAX_MEMBER_ASSIGNMENT_PARTITIONS);
    if !feasible_with_max_load(eligibility, topic_partition_counts, members, high)? {
        return Err(AssignorError::CapacityExceeded);
    }
    let mut low = 0_usize;
    let mut high = high;
    while low < high {
        let mid = low + (high - low) / 2;
        if feasible_with_max_load(eligibility, topic_partition_counts, members, mid)? {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    Ok(low)
}

fn feasible_with_max_load(
    eligibility: &BTreeMap<TopicId, Vec<MemberId>>,
    topic_partition_counts: &BTreeMap<TopicId, u32>,
    members: &BTreeMap<MemberId, MemberRecordV1>,
    max_load: usize,
) -> AssignorResult<bool> {
    let topics = eligibility.keys().copied().collect::<Vec<_>>();
    let member_ids = members.keys().copied().collect::<Vec<_>>();
    let source = 0;
    let topic_base = 1;
    let member_base = topic_base + topics.len();
    let sink = member_base + member_ids.len();
    let mut graph = Dinic::new(sink + 1);
    let member_index = member_ids
        .iter()
        .enumerate()
        .map(|(index, member_id)| (*member_id, member_base + index))
        .collect::<BTreeMap<_, _>>();
    let mut target = 0_u32;

    for (topic_offset, topic_id) in topics.iter().enumerate() {
        let count = *topic_partition_counts
            .get(topic_id)
            .ok_or(AssignorError::NoFeasibleCompletion)?;
        target = target
            .checked_add(count)
            .ok_or(AssignorError::ArithmeticOverflow)?;
        let topic_node = topic_base + topic_offset;
        graph.add_edge(source, topic_node, count);
        for member_id in &eligibility[topic_id] {
            graph.add_edge(topic_node, member_index[member_id], count);
        }
    }
    let cap = u32::try_from(max_load).map_err(|_| AssignorError::ArithmeticOverflow)?;
    for member_id in member_ids {
        graph.add_edge(member_index[&member_id], sink, cap);
    }
    Ok(graph.max_flow(source, sink) == target)
}

fn minimum_remaining_movement(
    eligibility: &BTreeMap<TopicId, Vec<MemberId>>,
    remaining_counts: &BTreeMap<TopicId, u32>,
    remaining_current: &BTreeMap<(TopicId, MemberId), u32>,
    remaining_capacities: &BTreeMap<MemberId, u32>,
    remaining_lower: &BTreeMap<MemberId, u32>,
) -> AssignorResult<Option<usize>> {
    let total = remaining_counts.values().try_fold(0_u32, |total, count| {
        total
            .checked_add(*count)
            .ok_or(AssignorError::ArithmeticOverflow)
    })?;
    if total == 0 {
        return Ok((remaining_lower.values().all(|value| *value == 0)).then_some(0));
    }

    let topics = remaining_counts
        .iter()
        .filter_map(|(topic_id, count)| (*count > 0).then_some(*topic_id))
        .collect::<Vec<_>>();
    let members = remaining_capacities.keys().copied().collect::<Vec<_>>();
    let source = 0;
    let topic_base = 1;
    let member_base = topic_base + topics.len();
    let sink = member_base + members.len();
    let mut graph = MinCostFlow::new(sink + 1);
    let member_index = members
        .iter()
        .enumerate()
        .map(|(index, member_id)| (*member_id, member_base + index))
        .collect::<BTreeMap<_, _>>();
    let big = i64::from(total) + 1;

    for (topic_offset, topic_id) in topics.iter().enumerate() {
        let count = remaining_counts[topic_id];
        let topic_node = topic_base + topic_offset;
        graph.add_edge(source, topic_node, count, 0);
        let Some(eligible) = eligibility.get(topic_id) else {
            return Ok(None);
        };
        for member_id in eligible {
            let retain = remaining_current
                .get(&(*topic_id, *member_id))
                .copied()
                .unwrap_or(0)
                .min(count);
            if retain > 0 {
                graph.add_edge(topic_node, member_index[member_id], retain, 0);
            }
            graph.add_edge(topic_node, member_index[member_id], count, 1);
        }
    }

    let mut lower_sum = 0_u32;
    for member_id in members {
        let capacity = remaining_capacities[&member_id];
        let lower = remaining_lower.get(&member_id).copied().unwrap_or(0);
        if lower > capacity {
            return Ok(None);
        }
        lower_sum = lower_sum
            .checked_add(lower)
            .ok_or(AssignorError::ArithmeticOverflow)?;
        if lower > 0 {
            graph.add_edge(member_index[&member_id], sink, lower, 0);
        }
        if capacity > lower {
            graph.add_edge(member_index[&member_id], sink, capacity - lower, big);
        }
    }
    if lower_sum > total {
        return Ok(None);
    }

    let Some((flow, raw_cost)) = graph.min_cost_flow(source, sink, total)? else {
        return Ok(None);
    };
    if flow != total {
        return Ok(None);
    }
    let expected_optional = total - lower_sum;
    let optional_cost = i64::from(expected_optional)
        .checked_mul(big)
        .ok_or(AssignorError::ArithmeticOverflow)?;
    let excess_cost = raw_cost
        .checked_sub(optional_cost)
        .ok_or(AssignorError::ArithmeticOverflow)?;
    if !(0..big).contains(&excess_cost) {
        return Ok(None);
    }

    let eligible_current = remaining_current.values().try_fold(0_u32, |total, value| {
        total
            .checked_add(*value)
            .ok_or(AssignorError::ArithmeticOverflow)
    })?;
    let no_eligible_current = total
        .checked_sub(eligible_current)
        .ok_or(AssignorError::ArithmeticOverflow)?;
    let excess = u32::try_from(excess_cost).map_err(|_| AssignorError::ArithmeticOverflow)?;
    let movement = excess
        .checked_sub(no_eligible_current)
        .ok_or(AssignorError::NoFeasibleCompletion)?;
    Ok(Some(
        usize::try_from(movement).map_err(|_| AssignorError::ArithmeticOverflow)?,
    ))
}

fn assignment_max_load(
    assignments: &BTreeMap<TopicPartitionV0, MemberId>,
    members: &BTreeMap<MemberId, MemberRecordV1>,
) -> usize {
    let mut loads = members
        .keys()
        .copied()
        .map(|member_id| (member_id, 0_usize))
        .collect::<BTreeMap<_, _>>();
    for member_id in assignments.values() {
        *loads.entry(*member_id).or_default() += 1;
    }
    loads.values().copied().max().unwrap_or(0)
}

#[derive(Clone)]
struct DinicEdge {
    to: usize,
    rev: usize,
    cap: u32,
}

struct Dinic {
    graph: Vec<Vec<DinicEdge>>,
}

impl Dinic {
    fn new(nodes: usize) -> Self {
        Self {
            graph: vec![Vec::new(); nodes],
        }
    }

    fn add_edge(&mut self, from: usize, to: usize, cap: u32) {
        let forward_rev = self.graph[to].len();
        let reverse_rev = self.graph[from].len();
        self.graph[from].push(DinicEdge {
            to,
            rev: forward_rev,
            cap,
        });
        self.graph[to].push(DinicEdge {
            to: from,
            rev: reverse_rev,
            cap: 0,
        });
    }

    fn max_flow(&mut self, source: usize, sink: usize) -> u32 {
        let mut total = 0_u32;
        loop {
            let mut level = vec![-1_i32; self.graph.len()];
            let mut queue = VecDeque::new();
            level[source] = 0;
            queue.push_back(source);
            while let Some(node) = queue.pop_front() {
                for edge in &self.graph[node] {
                    if edge.cap > 0 && level[edge.to] < 0 {
                        level[edge.to] = level[node] + 1;
                        queue.push_back(edge.to);
                    }
                }
            }
            if level[sink] < 0 {
                break;
            }
            let mut cursor = vec![0_usize; self.graph.len()];
            loop {
                let pushed = self.dfs(source, sink, u32::MAX, &level, &mut cursor);
                if pushed == 0 {
                    break;
                }
                total = total.saturating_add(pushed);
            }
        }
        total
    }

    fn dfs(
        &mut self,
        node: usize,
        sink: usize,
        flow: u32,
        level: &[i32],
        cursor: &mut [usize],
    ) -> u32 {
        if node == sink {
            return flow;
        }
        while cursor[node] < self.graph[node].len() {
            let edge_index = cursor[node];
            let edge = self.graph[node][edge_index].clone();
            if edge.cap > 0 && level[edge.to] == level[node] + 1 {
                let pushed = self.dfs(edge.to, sink, flow.min(edge.cap), level, cursor);
                if pushed > 0 {
                    let reverse = edge.rev;
                    self.graph[node][edge_index].cap -= pushed;
                    self.graph[edge.to][reverse].cap += pushed;
                    return pushed;
                }
            }
            cursor[node] += 1;
        }
        0
    }
}

#[derive(Clone)]
struct CostEdge {
    to: usize,
    rev: usize,
    cap: u32,
    cost: i64,
}

struct MinCostFlow {
    graph: Vec<Vec<CostEdge>>,
}

impl MinCostFlow {
    fn new(nodes: usize) -> Self {
        Self {
            graph: vec![Vec::new(); nodes],
        }
    }

    fn add_edge(&mut self, from: usize, to: usize, cap: u32, cost: i64) {
        let forward_rev = self.graph[to].len();
        let reverse_rev = self.graph[from].len();
        self.graph[from].push(CostEdge {
            to,
            rev: forward_rev,
            cap,
            cost,
        });
        self.graph[to].push(CostEdge {
            to: from,
            rev: reverse_rev,
            cap: 0,
            cost: -cost,
        });
    }

    fn min_cost_flow(
        &mut self,
        source: usize,
        sink: usize,
        target: u32,
    ) -> AssignorResult<Option<(u32, i64)>> {
        let nodes = self.graph.len();
        let mut potential = vec![0_i64; nodes];
        let mut flow = 0_u32;
        let mut cost = 0_i64;
        const INF: i64 = i64::MAX / 4;

        while flow < target {
            let mut dist = vec![INF; nodes];
            let mut previous = vec![None::<(usize, usize)>; nodes];
            let mut heap = BinaryHeap::new();
            dist[source] = 0;
            heap.push(Reverse((0_i64, source)));

            while let Some(Reverse((distance, node))) = heap.pop() {
                if distance != dist[node] {
                    continue;
                }
                for (edge_index, edge) in self.graph[node].iter().enumerate() {
                    if edge.cap == 0 {
                        continue;
                    }
                    let reduced = edge
                        .cost
                        .checked_add(potential[node])
                        .and_then(|value| value.checked_sub(potential[edge.to]))
                        .ok_or(AssignorError::ArithmeticOverflow)?;
                    if reduced < 0 {
                        return Err(AssignorError::NoFeasibleCompletion);
                    }
                    let candidate = distance
                        .checked_add(reduced)
                        .ok_or(AssignorError::ArithmeticOverflow)?;
                    if candidate < dist[edge.to] {
                        dist[edge.to] = candidate;
                        previous[edge.to] = Some((node, edge_index));
                        heap.push(Reverse((candidate, edge.to)));
                    }
                }
            }
            if dist[sink] == INF {
                return Ok(None);
            }
            for node in 0..nodes {
                if dist[node] != INF {
                    potential[node] = potential[node]
                        .checked_add(dist[node])
                        .ok_or(AssignorError::ArithmeticOverflow)?;
                }
            }

            let mut add = target - flow;
            let mut node = sink;
            while node != source {
                let Some((parent, edge_index)) = previous[node] else {
                    return Ok(None);
                };
                add = add.min(self.graph[parent][edge_index].cap);
                node = parent;
            }
            node = sink;
            while node != source {
                let (parent, edge_index) =
                    previous[node].ok_or(AssignorError::NoFeasibleCompletion)?;
                let edge = self.graph[parent][edge_index].clone();
                let edge_cost = edge
                    .cost
                    .checked_mul(i64::from(add))
                    .ok_or(AssignorError::ArithmeticOverflow)?;
                cost = cost
                    .checked_add(edge_cost)
                    .ok_or(AssignorError::ArithmeticOverflow)?;
                self.graph[parent][edge_index].cap -= add;
                self.graph[node][edge.rev].cap += add;
                node = parent;
            }
            flow = flow
                .checked_add(add)
                .ok_or(AssignorError::ArithmeticOverflow)?;
        }
        Ok(Some((flow, cost)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use astralog_protocol::api::MemberEpoch;

    type ExhaustiveCandidate = (
        usize,
        usize,
        Vec<MemberId>,
        BTreeMap<TopicPartitionV0, MemberId>,
    );

    fn topic(byte: u8) -> TopicId {
        TopicId::new([byte; 16])
    }

    fn member(byte: u8, subscriptions: Vec<TopicId>) -> MemberRecordV1 {
        let member_id = MemberId::new([byte; 16]).unwrap();
        MemberRecordV1 {
            member_id,
            instance_id: None,
            member_epoch: MemberEpoch::new(1).unwrap(),
            session_timeout_ms: 30_000,
            subscriptions,
        }
    }

    fn members(records: Vec<MemberRecordV1>) -> BTreeMap<MemberId, MemberRecordV1> {
        records
            .into_iter()
            .map(|record| (record.member_id, record))
            .collect()
    }

    fn tp(topic_id: TopicId, partition: u32) -> TopicPartitionV0 {
        TopicPartitionV0 {
            topic_id,
            partition,
        }
    }

    fn objective(
        assignment: &BTreeMap<TopicPartitionV0, MemberId>,
        members: &BTreeMap<MemberId, MemberRecordV1>,
        current: &BTreeMap<TopicPartitionV0, MemberId>,
    ) -> (usize, usize, Vec<MemberId>) {
        let max_load = assignment_max_load(assignment, members);
        let movement = assignment
            .iter()
            .filter(|(partition, new_owner)| {
                current.get(partition).is_some_and(|old_owner| {
                    member_is_eligible(members, *old_owner, partition.topic_id)
                        && old_owner != *new_owner
                })
            })
            .count();
        let owner_vector = assignment.values().copied().collect::<Vec<_>>();
        (max_load, movement, owner_vector)
    }

    fn exhaustive_best(
        members: &BTreeMap<MemberId, MemberRecordV1>,
        current: &BTreeMap<TopicPartitionV0, MemberId>,
        counts: &BTreeMap<TopicId, u32>,
    ) -> Option<BTreeMap<TopicPartitionV0, MemberId>> {
        let partitions = counts
            .iter()
            .flat_map(|(topic_id, count)| (0..*count).map(|partition| tp(*topic_id, partition)))
            .collect::<Vec<_>>();
        let same = members.values().next().is_some_and(|first| {
            members
                .values()
                .all(|m| m.subscriptions == first.subscriptions)
        });
        let mut best: Option<ExhaustiveCandidate> = None;
        let mut work = BTreeMap::new();

        fn recurse(
            index: usize,
            partitions: &[TopicPartitionV0],
            members: &BTreeMap<MemberId, MemberRecordV1>,
            current: &BTreeMap<TopicPartitionV0, MemberId>,
            same: bool,
            work: &mut BTreeMap<TopicPartitionV0, MemberId>,
            best: &mut Option<ExhaustiveCandidate>,
        ) {
            if index == partitions.len() {
                if work
                    .values()
                    .fold(BTreeMap::<MemberId, usize>::new(), |mut loads, owner| {
                        *loads.entry(*owner).or_default() += 1;
                        loads
                    })
                    .values()
                    .any(|load| *load > MAX_MEMBER_ASSIGNMENT_PARTITIONS)
                {
                    return;
                }
                if same && !members.is_empty() {
                    let mut loads = members
                        .keys()
                        .copied()
                        .map(|member_id| (member_id, 0_usize))
                        .collect::<BTreeMap<_, _>>();
                    for owner in work.values() {
                        *loads.entry(*owner).or_default() += 1;
                    }
                    let min = loads.values().copied().min().unwrap_or(0);
                    let max = loads.values().copied().max().unwrap_or(0);
                    if max.saturating_sub(min) > 1 {
                        return;
                    }
                }
                let (max_load, movement, owners) = objective(work, members, current);
                let candidate = (max_load, movement, owners, work.clone());
                let is_better = best.as_ref().is_none_or(|current_best| {
                    (candidate.0, candidate.1, &candidate.2)
                        < (current_best.0, current_best.1, &current_best.2)
                });
                if is_better {
                    *best = Some(candidate);
                }
                return;
            }
            let partition = partitions[index];
            for (member_id, member) in members {
                if member
                    .subscriptions
                    .binary_search(&partition.topic_id)
                    .is_ok()
                {
                    work.insert(partition, *member_id);
                    recurse(index + 1, partitions, members, current, same, work, best);
                    work.remove(&partition);
                }
            }
        }

        recurse(0, &partitions, members, current, same, &mut work, &mut best);
        best.map(|(_, _, _, assignment)| assignment)
    }

    #[test]
    fn assignor_matches_exhaustive_frozen_objective_on_small_state_space() {
        let a = MemberId::new([1; 16]).unwrap();
        let b = MemberId::new([2; 16]).unwrap();
        let c = MemberId::new([3; 16]).unwrap();
        let t1 = topic(1);
        let t2 = topic(2);
        let subscription_sets = [vec![t1], vec![t2], vec![t1, t2]];
        let current_patterns = [
            vec![Some(a), Some(b), Some(c), None],
            vec![Some(b), Some(b), Some(a), Some(c)],
            vec![None, Some(c), Some(b), Some(a)],
        ];
        let counts = BTreeMap::from([(t1, 2), (t2, 2)]);

        for left in &subscription_sets {
            for middle in &subscription_sets {
                for right in &subscription_sets {
                    let members = members(vec![
                        member(1, left.clone()),
                        member(2, middle.clone()),
                        member(3, right.clone()),
                    ]);
                    if [t1, t2].into_iter().any(|topic_id| {
                        !members
                            .values()
                            .any(|member| member.subscriptions.binary_search(&topic_id).is_ok())
                    }) {
                        continue;
                    }
                    for pattern in &current_patterns {
                        let partitions = [tp(t1, 0), tp(t1, 1), tp(t2, 0), tp(t2, 1)];
                        let current = partitions
                            .into_iter()
                            .zip(pattern.iter().copied())
                            .filter_map(|(partition, owner)| owner.map(|owner| (partition, owner)))
                            .collect::<BTreeMap<_, _>>();
                        let expected = exhaustive_best(&members, &current, &counts).unwrap();
                        let actual = uniform_sticky_assign(&members, &current, &counts).unwrap();
                        assert_eq!(actual.assignments, expected);
                    }
                }
            }
        }
    }

    #[test]
    fn same_subscription_members_are_floor_ceil_balanced_before_stickiness() {
        let t = topic(1);
        let members = members(vec![
            member(1, vec![t]),
            member(2, vec![t]),
            member(3, vec![t]),
        ]);
        let mut current = BTreeMap::new();
        current.insert(tp(t, 0), MemberId::new([1; 16]).unwrap());
        current.insert(tp(t, 1), MemberId::new([1; 16]).unwrap());
        current.insert(tp(t, 2), MemberId::new([2; 16]).unwrap());
        current.insert(tp(t, 3), MemberId::new([2; 16]).unwrap());
        let result = uniform_sticky_assign(&members, &current, &BTreeMap::from([(t, 4)])).unwrap();
        let mut loads = BTreeMap::<MemberId, usize>::new();
        for owner in result.assignments.values() {
            *loads.entry(*owner).or_default() += 1;
        }
        for member_id in members.keys() {
            loads.entry(*member_id).or_default();
        }
        assert_eq!(
            loads.values().copied().max().unwrap() - loads.values().copied().min().unwrap(),
            1
        );
        assert_eq!(result.max_load, 2);
        assert_eq!(result.movement_count, 1);
    }

    #[test]
    fn eligibility_is_absolute_and_ineligible_current_owner_has_no_stickiness_credit() {
        let t1 = topic(1);
        let t2 = topic(2);
        let members = members(vec![member(1, vec![t1]), member(2, vec![t2])]);
        let current = BTreeMap::from([
            (tp(t1, 0), MemberId::new([2; 16]).unwrap()),
            (tp(t2, 0), MemberId::new([1; 16]).unwrap()),
        ]);
        let result =
            uniform_sticky_assign(&members, &current, &BTreeMap::from([(t1, 1), (t2, 1)])).unwrap();
        assert_eq!(
            result.assignments[&tp(t1, 0)],
            MemberId::new([1; 16]).unwrap()
        );
        assert_eq!(
            result.assignments[&tp(t2, 0)],
            MemberId::new([2; 16]).unwrap()
        );
        assert_eq!(result.movement_count, 0);
    }

    #[test]
    fn reordered_map_insertion_produces_identical_assignment() {
        let t = topic(1);
        let first = members(vec![
            member(1, vec![t]),
            member(2, vec![t]),
            member(3, vec![t]),
        ]);
        let second = members(vec![
            member(3, vec![t]),
            member(1, vec![t]),
            member(2, vec![t]),
        ]);
        let counts = BTreeMap::from([(t, 7)]);
        let a = uniform_sticky_assign(&first, &BTreeMap::new(), &counts).unwrap();
        let b = uniform_sticky_assign(&second, &BTreeMap::new(), &counts).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn member_join_and_leave_move_only_what_frozen_balance_requires() {
        let t = topic(1);
        let a = MemberId::new([1; 16]).unwrap();
        let b = MemberId::new([2; 16]).unwrap();
        let c = MemberId::new([3; 16]).unwrap();
        let current = BTreeMap::from([
            (tp(t, 0), a),
            (tp(t, 1), a),
            (tp(t, 2), b),
            (tp(t, 3), b),
            (tp(t, 4), b),
            (tp(t, 5), a),
        ]);
        let three = members(vec![
            member(1, vec![t]),
            member(2, vec![t]),
            member(3, vec![t]),
        ]);
        let joined = uniform_sticky_assign(&three, &current, &BTreeMap::from([(t, 6)])).unwrap();
        assert_eq!(joined.movement_count, 2);
        assert_eq!(
            joined
                .assignments
                .values()
                .filter(|owner| **owner == c)
                .count(),
            2
        );

        let remaining = members(vec![member(1, vec![t]), member(3, vec![t])]);
        let left =
            uniform_sticky_assign(&remaining, &joined.assignments, &BTreeMap::from([(t, 6)]))
                .unwrap();
        assert_eq!(
            left.assignments
                .values()
                .filter(|owner| **owner == b)
                .count(),
            0
        );
        assert_eq!(left.max_load, 3);
    }

    #[test]
    fn partition_expansion_preserves_old_ownership_when_balance_does_not_require_movement() {
        let t = topic(1);
        let a = MemberId::new([1; 16]).unwrap();
        let b = MemberId::new([2; 16]).unwrap();
        let members = members(vec![member(1, vec![t]), member(2, vec![t])]);
        let current = BTreeMap::from([(tp(t, 0), a), (tp(t, 1), b)]);
        let result = uniform_sticky_assign(&members, &current, &BTreeMap::from([(t, 4)])).unwrap();
        assert_eq!(result.assignments[&tp(t, 0)], a);
        assert_eq!(result.assignments[&tp(t, 1)], b);
        assert_eq!(result.movement_count, 0);
        assert_eq!(result.max_load, 2);
    }

    #[test]
    fn bounds_and_unsatisfiable_inputs_fail_explicitly() {
        let t = topic(1);
        let no_subscriber = members(vec![member(1, Vec::new())]);
        assert!(matches!(
            uniform_sticky_assign(&no_subscriber, &BTreeMap::new(), &BTreeMap::from([(t, 1)])),
            Err(AssignorError::NoEligibleOwner { .. })
        ));

        let members = members(vec![member(1, vec![t]), member(2, vec![t])]);
        assert!(matches!(
            uniform_sticky_assign(
                &members,
                &BTreeMap::new(),
                &BTreeMap::from([(t, u32::try_from(MAX_GROUP_PARTITIONS + 1).unwrap())]),
            ),
            Err(AssignorError::TooManyPartitions)
        ));
        assert!(matches!(
            uniform_sticky_assign(&members, &BTreeMap::new(), &BTreeMap::from([(t, 2_049)]),),
            Err(AssignorError::CapacityExceeded)
        ));
    }
}
