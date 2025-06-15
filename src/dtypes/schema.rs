use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Represents the five possible SQLite data type affinities.
///
/// SQLite uses dynamic typing and internally categorizes column types
/// into one of the following affinities, regardless of the exact type
/// name specified in a `CREATE TABLE` statement or `CAST` expression.
///
/// The affinity of a column affects how values are stored and compared.
/// This enum reflects those internal affinity categories.
///
/// See: <https://www.sqlite.org/datatype3.html>
pub enum DataType {
    /// `Null` indicates no type affinity.
    /// Values stored in this column can be of any type.
    Null,

    /// `Text` affinity is applied to types like:
    /// `TEXT`, `CHARACTER`, `VARCHAR`, `CLOB`, `NCHAR`, `NVARCHAR`, etc.
    ///
    /// Values are stored as strings.
    ///
    /// Determined by rule 2: if the type contains "CHAR", "CLOB", or "TEXT".
    Text,

    /// `Int` (INTEGER affinity) is applied to types such as:
    /// `INT`, `INTEGER`, `BIGINT`, `TINYINT`, `SMALLINT`, `UNSIGNED BIG INT`, etc.
    ///
    /// Values are stored as integers when possible.
    ///
    /// Determined by rule 1: if the type contains "INT".
    Int,

    /// `Real` (REAL affinity) applies to types like:
    /// `REAL`, `DOUBLE`, `DOUBLE PRECISION`, `FLOAT`.
    ///
    /// Values are stored as floating-point numbers.
    ///
    /// Determined by rule 4: if the type contains "REAL", "FLOA", or "DOUB".
    Real,

    /// `Numeric` affinity is assigned to types such as:
    /// `NUMERIC`, `DECIMAL`, `BOOLEAN`, `DATE`, `DATETIME`, etc.
    ///
    /// Values are stored using the type of the value itself, possibly as integers,
    /// reals, or text depending on context.
    ///
    /// Determined by rule 5: if it contains "NUM", "DEC", "BOOL", "DATE", etc.
    Numeric,

    /// `Blob` affinity means no conversion is ever applied.
    ///
    /// Applies to type `BLOB`, or when no type is specified in the schema.
    ///
    /// Determined by rule 3: if none of the other rules match.
    Blob,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            DataType::Null => "NULL",
            DataType::Int => "INTEGER",
            DataType::Blob => "BLOB",
            DataType::Numeric => "NUMERIC",
            DataType::Real => "REAL",
            DataType::Text => "TEXT",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone)]
pub enum ValueLiteral {
    Null,
    Boolean(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl ValueLiteral {
    pub fn len(&self) -> usize {
        match self {
            ValueLiteral::Null => 0,
            ValueLiteral::Boolean(_) => std::mem::size_of::<bool>(),
            ValueLiteral::Int(_) => std::mem::size_of::<i64>(),
            ValueLiteral::Float(_) => std::mem::size_of::<f64>(),
            ValueLiteral::Text(s) => s.len(),
            ValueLiteral::Blob(b) => b.len(),
        }
    }

    pub fn as_null(&self) -> Option<()> {
        match self {
            ValueLiteral::Null => Some(()),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            ValueLiteral::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            ValueLiteral::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            ValueLiteral::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            ValueLiteral::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            ValueLiteral::Blob(b) => Some(b.as_slice()),
            _ => None,
        }
    }
}

impl fmt::Display for ValueLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueLiteral::Null => write!(f, "NULL"),
            ValueLiteral::Boolean(b) => write!(f, "{}", b),
            ValueLiteral::Int(i) => write!(f, "{}", i),
            ValueLiteral::Float(n) => write!(f, "{}", n),
            ValueLiteral::Text(s) => write!(f, "{}", s),
            ValueLiteral::Blob(bytes) => {
                write!(f, "0x")?;
                for byte in bytes {
                    write!(f, "{:02x}", byte)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub struct TypedValue {
    pub dtype: DataType,
    pub value: ValueLiteral,
}

impl fmt::Display for TypedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.value {
            ValueLiteral::Null => write!(f, "NULL"),
            _ => write!(f, "{} ({})", self.value, self.dtype),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchemaField {
    pub name: String,
    pub dtype: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub fields: Vec<SchemaField>,
}

impl Schema {
    pub fn field_names(&self) -> Vec<String> {
        self.fields.iter().map(|f| f.name.clone()).collect()
    }

    pub fn field_types(&self) -> Vec<DataType> {
        self.fields.iter().map(|f| f.dtype.clone()).collect()
    }
}
