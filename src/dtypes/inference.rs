use crate::dtypes::schema::DataType;

#[derive(Debug, Clone, PartialEq)]
pub enum InferredType {
    Null,
    Bool,
    Int,
    Float,
    String,
}

impl InferredType {
    pub fn update(&mut self, value: &str) {
        let val = value.trim();

        if val.is_empty() {
            return; // Null - don't promote
        }

        let new_type = if val.eq_ignore_ascii_case("true") || val.eq_ignore_ascii_case("false") {
            InferredType::Bool
        } else if val.parse::<i64>().is_ok() {
            InferredType::Int
        } else if val.parse::<f64>().is_ok() {
            InferredType::Float
        } else {
            InferredType::String
        };

        *self = Self::promote(self, &new_type);
    }

    fn promote(current: &InferredType, new: &InferredType) -> InferredType {
        use InferredType::*;
        match (current, new) {
            (String, _) | (_, String) => String,
            (Float, _) | (_, Float) => Float,
            (Int, _) | (_, Int) => Int,
            (Bool, _) | (_, Bool) => Bool,
            (Null, other) => other.clone(),
        }
    }

    pub fn as_str(&self, nullable: bool) -> String {
        let base = match self {
            InferredType::Null => "null",
            InferredType::Bool => "bool",
            InferredType::Int => "int",
            InferredType::Float => "float",
            InferredType::String => "string",
        };

        if nullable && *self != InferredType::Null {
            format!("nullable<{}>", base)
        } else {
            base.to_string()
        }
    }

    pub fn to_data_type(&self) -> DataType {
        match self {
            InferredType::Null => DataType::Null,
            InferredType::Bool => DataType::Boolean,
            InferredType::Int => DataType::Int,
            InferredType::Float => DataType::Float,
            InferredType::String => DataType::String,
        }
    }
}

#[cfg(test)]
mod inferred_type_tests {
    use super::InferredType;

    #[test]
    fn test_initial_null_remains_null_with_empty() {
        let mut t = InferredType::Null;
        t.update("");
        t.update("   ");
        assert_eq!(t, InferredType::Null);
    }

    #[test]
    fn test_null_to_int_promotion() {
        let mut t = InferredType::Null;
        t.update("42");
        assert_eq!(t, InferredType::Int);
    }

    #[test]
    fn test_null_to_float_promotion() {
        let mut t = InferredType::Null;
        t.update("3.14");
        assert_eq!(t, InferredType::Float);
    }

    #[test]
    fn test_null_to_bool_promotion() {
        let mut t = InferredType::Null;
        t.update("TRUE");
        assert_eq!(t, InferredType::Bool);
    }

    #[test]
    fn test_null_to_string_promotion() {
        let mut t = InferredType::Null;
        t.update("hello");
        assert_eq!(t, InferredType::String);
    }

    #[test]
    fn test_int_promotes_to_float() {
        let mut t = InferredType::Int;
        t.update("3.5");
        assert_eq!(t, InferredType::Float);
    }

    #[test]
    fn test_float_promotes_to_string() {
        let mut t = InferredType::Float;
        t.update("not a number");
        assert_eq!(t, InferredType::String);
    }

    #[test]
    fn test_bool_promotes_to_string() {
        let mut t = InferredType::Bool;
        t.update("yes"); // not a valid boolean
        assert_eq!(t, InferredType::String);
    }

    #[test]
    fn test_update_chain_mixed_values() {
        let mut t = InferredType::Null;
        t.update("42");
        t.update("3.14");
        t.update("true");
        t.update("hello");
        assert_eq!(t, InferredType::String);
    }

    #[test]
    fn test_as_str_non_nullable() {
        assert_eq!(InferredType::Int.as_str(false), "int");
        assert_eq!(InferredType::Float.as_str(false), "float");
        assert_eq!(InferredType::Null.as_str(false), "null");
    }

    #[test]
    fn test_as_str_nullable() {
        assert_eq!(InferredType::Int.as_str(true), "nullable<int>");
        assert_eq!(InferredType::Bool.as_str(true), "nullable<bool>");
        assert_eq!(InferredType::Null.as_str(true), "null"); // null stays null
    }
}
