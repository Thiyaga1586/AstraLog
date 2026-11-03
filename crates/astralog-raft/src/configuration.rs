use std::collections::BTreeSet;

use crate::{NodeId, RaftError, Result, error::NodeIdValue};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StableConfiguration {
    voters: BTreeSet<NodeId>,
    learners: BTreeSet<NodeId>,
}

impl StableConfiguration {
    pub fn new(
        voters: impl IntoIterator<Item = NodeId>,
        learners: impl IntoIterator<Item = NodeId>,
    ) -> Result<Self> {
        let voters = collect_unique_members(voters)?;
        if voters.is_empty() {
            return Err(RaftError::EmptyVoterSet);
        }
        let learners = collect_unique_members(learners)?;
        if let Some(overlap) = voters.intersection(&learners).next().copied() {
            return Err(RaftError::MemberInBothRoles(NodeIdValue(overlap.get())));
        }
        Ok(Self { voters, learners })
    }

    pub fn voters(&self) -> impl ExactSizeIterator<Item = NodeId> + '_ {
        self.voters.iter().copied()
    }

    pub fn learners(&self) -> impl ExactSizeIterator<Item = NodeId> + '_ {
        self.learners.iter().copied()
    }

    pub fn contains_voter(&self, node_id: NodeId) -> bool {
        self.voters.contains(&node_id)
    }

    pub fn contains_learner(&self, node_id: NodeId) -> bool {
        self.learners.contains(&node_id)
    }

    pub fn contains(&self, node_id: NodeId) -> bool {
        self.contains_voter(node_id) || self.contains_learner(node_id)
    }

    pub fn voter_count(&self) -> usize {
        self.voters.len()
    }

    pub fn quorum_size(&self) -> usize {
        self.voters.len() / 2 + 1
    }
}

fn collect_unique_members(members: impl IntoIterator<Item = NodeId>) -> Result<BTreeSet<NodeId>> {
    let mut unique = BTreeSet::new();
    for member in members {
        if !unique.insert(member) {
            return Err(RaftError::DuplicateMember(NodeIdValue(member.get())));
        }
    }
    Ok(unique)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JointConfiguration {
    old: StableConfiguration,
    new: StableConfiguration,
}

impl JointConfiguration {
    pub fn new(old: StableConfiguration, new: StableConfiguration) -> Result<Self> {
        if old == new {
            return Err(RaftError::InvalidJointConfiguration);
        }
        Ok(Self { old, new })
    }

    pub const fn old(&self) -> &StableConfiguration {
        &self.old
    }

    pub const fn new_config(&self) -> &StableConfiguration {
        &self.new
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectiveConfiguration {
    Stable(StableConfiguration),
    Joint(JointConfiguration),
}

impl EffectiveConfiguration {
    pub fn contains_voter(&self, node_id: NodeId) -> bool {
        match self {
            Self::Stable(config) => config.contains_voter(node_id),
            Self::Joint(config) => {
                config.old.contains_voter(node_id) || config.new.contains_voter(node_id)
            }
        }
    }

    pub fn contains_learner(&self, node_id: NodeId) -> bool {
        match self {
            Self::Stable(config) => config.contains_learner(node_id),
            Self::Joint(config) => {
                !self.contains_voter(node_id)
                    && (config.old.contains_learner(node_id)
                        || config.new.contains_learner(node_id))
            }
        }
    }

    pub fn all_members(&self) -> BTreeSet<NodeId> {
        let mut members = BTreeSet::new();
        match self {
            Self::Stable(config) => {
                members.extend(config.voters());
                members.extend(config.learners());
            }
            Self::Joint(config) => {
                members.extend(config.old.voters());
                members.extend(config.old.learners());
                members.extend(config.new.voters());
                members.extend(config.new.learners());
            }
        }
        members
    }

    pub fn voters(&self) -> BTreeSet<NodeId> {
        match self {
            Self::Stable(config) => config.voters().collect(),
            Self::Joint(config) => config.old.voters().chain(config.new.voters()).collect(),
        }
    }

    pub fn has_election_quorum(&self, granted: &BTreeSet<NodeId>) -> bool {
        self.has_voter_quorum(granted)
    }

    pub fn has_voter_quorum(&self, members: &BTreeSet<NodeId>) -> bool {
        match self {
            Self::Stable(config) => has_majority(config, members),
            Self::Joint(config) => {
                has_majority(config.old(), members) && has_majority(config.new_config(), members)
            }
        }
    }

    pub fn replication_quorum_at(
        &self,
        index: crate::LogIndex,
        progress: &std::collections::BTreeMap<NodeId, crate::Progress>,
    ) -> bool {
        let replicated = self
            .voters()
            .into_iter()
            .filter(|node| {
                progress
                    .get(node)
                    .is_some_and(|peer| peer.match_index >= index)
            })
            .collect();
        self.has_voter_quorum(&replicated)
    }
}

fn has_majority(config: &StableConfiguration, granted: &BTreeSet<NodeId>) -> bool {
    config
        .voters()
        .filter(|node| granted.contains(node))
        .count()
        >= config.quorum_size()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigurationChange {
    EnterJoint {
        old: StableConfiguration,
        new: StableConfiguration,
    },
    LeaveJoint {
        new: StableConfiguration,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn node(value: u64) -> NodeId {
        NodeId::new(value).unwrap()
    }

    #[test]
    fn stable_configuration_requires_voters_and_disjoint_roles() {
        assert_eq!(
            StableConfiguration::new([], []),
            Err(RaftError::EmptyVoterSet)
        );
        assert_eq!(
            StableConfiguration::new([node(1)], [node(1)]),
            Err(RaftError::MemberInBothRoles(NodeIdValue(1)))
        );
        assert_eq!(
            StableConfiguration::new([node(1), node(1)], []),
            Err(RaftError::DuplicateMember(NodeIdValue(1)))
        );
        assert_eq!(
            StableConfiguration::new([node(1)], [node(2), node(2)]),
            Err(RaftError::DuplicateMember(NodeIdValue(2)))
        );
    }

    #[test]
    fn stable_configuration_iteration_is_deterministic() {
        let config =
            StableConfiguration::new([node(3), node(1), node(2)], [node(5), node(4)]).unwrap();
        assert_eq!(
            config.voters().map(NodeId::get).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            config.learners().map(NodeId::get).collect::<Vec<_>>(),
            vec![4, 5]
        );
        assert_eq!(config.quorum_size(), 2);
    }

    #[test]
    fn joint_membership_unions_authority_without_promoting_learners() {
        let old = StableConfiguration::new([node(1), node(2), node(3)], [node(4)]).unwrap();
        let new = StableConfiguration::new([node(2), node(3), node(4)], [node(5)]).unwrap();
        let effective = EffectiveConfiguration::Joint(JointConfiguration::new(old, new).unwrap());
        assert!(effective.contains_voter(node(1)));
        assert!(effective.contains_voter(node(4)));
        assert!(effective.contains_learner(node(5)));
        assert!(!effective.contains_learner(node(4)));
        assert_eq!(
            effective
                .all_members()
                .into_iter()
                .map(NodeId::get)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
    }

    #[test]
    fn election_quorum_requires_both_joint_majorities() {
        let old = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let new = StableConfiguration::new([node(3), node(4), node(5)], []).unwrap();
        let effective = EffectiveConfiguration::Joint(JointConfiguration::new(old, new).unwrap());

        let old_only = [node(1), node(2), node(3)].into_iter().collect();
        assert!(!effective.has_election_quorum(&old_only));

        let both = [node(1), node(3), node(4)].into_iter().collect();
        assert!(effective.has_election_quorum(&both));
    }

    #[test]
    fn replication_quorum_excludes_learners_and_requires_both_joint_majorities() {
        let old = StableConfiguration::new([node(1), node(2), node(3)], [node(6)]).unwrap();
        let new = StableConfiguration::new([node(3), node(4), node(5)], [node(6)]).unwrap();
        let config = EffectiveConfiguration::Joint(JointConfiguration::new(old, new).unwrap());
        let mut progress = BTreeMap::new();
        for member in config.all_members() {
            progress.insert(
                member,
                crate::Progress {
                    match_index: crate::LogIndex::ZERO,
                    next_index: crate::LogIndex::FIRST,
                    recently_active: false,
                    mode: crate::ReplicationMode::Probe,
                },
            );
        }

        for member in [node(1), node(2), node(4), node(6)] {
            progress.get_mut(&member).unwrap().match_index = crate::LogIndex::FIRST;
        }
        assert!(!config.replication_quorum_at(crate::LogIndex::FIRST, &progress));

        progress.get_mut(&node(3)).unwrap().match_index = crate::LogIndex::FIRST;
        assert!(config.replication_quorum_at(crate::LogIndex::FIRST, &progress));
    }
}
