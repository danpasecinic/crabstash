use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    S,
    X,
    IS,
    IX,
    SIX,
}

impl LockMode {
    pub fn is_compatible(&self, other: &LockMode) -> bool {
        use LockMode::*;
        matches!(
            (self, other),
            (IS, IS)
                | (IS, IX)
                | (IS, S)
                | (IS, SIX)
                | (IX, IS)
                | (IX, IX)
                | (S, IS)
                | (S, S)
                | (SIX, IS)
        )
    }

    pub fn is_shared(&self) -> bool {
        matches!(self, LockMode::S | LockMode::IS)
    }

    pub fn is_exclusive(&self) -> bool {
        matches!(self, LockMode::X | LockMode::IX | LockMode::SIX)
    }

    pub fn can_upgrade_to(&self, target: &LockMode) -> bool {
        use LockMode::*;
        matches!(
            (self, target),
            (S, X) | (S, SIX) | (IS, IX) | (IS, S) | (IS, X) | (IS, SIX) | (IX, X) | (IX, SIX)
        )
    }
}

impl fmt::Display for LockMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockMode::S => write!(f, "S"),
            LockMode::X => write!(f, "X"),
            LockMode::IS => write!(f, "IS"),
            LockMode::IX => write!(f, "IX"),
            LockMode::SIX => write!(f, "SIX"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compatibility_matrix() {
        use LockMode::*;

        assert!(IS.is_compatible(&IS));
        assert!(IS.is_compatible(&IX));
        assert!(IS.is_compatible(&S));
        assert!(IS.is_compatible(&SIX));
        assert!(!IS.is_compatible(&X));

        assert!(IX.is_compatible(&IS));
        assert!(IX.is_compatible(&IX));
        assert!(!IX.is_compatible(&S));
        assert!(!IX.is_compatible(&SIX));
        assert!(!IX.is_compatible(&X));

        assert!(S.is_compatible(&IS));
        assert!(!S.is_compatible(&IX));
        assert!(S.is_compatible(&S));
        assert!(!S.is_compatible(&SIX));
        assert!(!S.is_compatible(&X));

        assert!(SIX.is_compatible(&IS));
        assert!(!SIX.is_compatible(&IX));
        assert!(!SIX.is_compatible(&S));
        assert!(!SIX.is_compatible(&SIX));
        assert!(!SIX.is_compatible(&X));

        assert!(!X.is_compatible(&IS));
        assert!(!X.is_compatible(&IX));
        assert!(!X.is_compatible(&S));
        assert!(!X.is_compatible(&SIX));
        assert!(!X.is_compatible(&X));
    }

    #[test]
    fn test_upgrade_paths() {
        use LockMode::*;

        assert!(S.can_upgrade_to(&X));
        assert!(S.can_upgrade_to(&SIX));
        assert!(IS.can_upgrade_to(&IX));
        assert!(IS.can_upgrade_to(&S));
        assert!(IS.can_upgrade_to(&X));
        assert!(IX.can_upgrade_to(&X));

        assert!(!X.can_upgrade_to(&S));
        assert!(!S.can_upgrade_to(&IS));
    }
}
