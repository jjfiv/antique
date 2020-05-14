use crate::scoring::EvalNode;

/// Core-related to Movement trait.
pub enum MoverType {
    AllMover,
    EmptyMover,
    RealMover(Box<dyn EvalNode>),
    And(Vec<MoverType>),
    Or(Vec<MoverType>),
}

impl MoverType {
    pub(crate) fn create_or(input: Vec<Self>) -> Self {
        // Ditch all empty-movers:
        let mut flattened = Vec::new();
        for it in input.into_iter() {
            match it {
                // (everything OR x) == everything
                MoverType::AllMover => return MoverType::AllMover,
                // (nothing OR x) == x
                MoverType::EmptyMover => continue,
                MoverType::Or(insides) => flattened.extend(insides),
                x => flattened.push(x),
            }
        }

        match flattened.len() {
            // TODO: think more about this case
            0 => MoverType::EmptyMover,
            1 => flattened.into_iter().next().unwrap(),
            _ => MoverType::Or(flattened),
        }
    }
    pub(crate) fn create_and(input: Vec<Self>) -> Self {
        let mut flattened = Vec::new();
        for it in input.into_iter() {
            match it {
                // (nothing AND x) == nothing
                MoverType::EmptyMover => return MoverType::EmptyMover,
                // (everything AND x) == x
                MoverType::AllMover => continue,
                MoverType::And(insides) => flattened.extend(insides),
                x => flattened.push(x),
            }
        }

        match flattened.len() {
            // TODO: think more about this case
            0 => MoverType::EmptyMover,
            1 => flattened.into_iter().next().unwrap(),
            _ => MoverType::And(flattened),
        }
    }
}
