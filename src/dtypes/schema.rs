#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Null,
    Boolean,
    Int,
    Float,
    String,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            DataType::Null => "null",
            DataType::Boolean => "bool",
            DataType::Int => "int",
            DataType::Float => "float",
            DataType::String => "string",
        };
        write!(f, "{}", s)
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
