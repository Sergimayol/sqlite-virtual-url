use std::error::Error;

#[derive(Debug, PartialEq)]
pub enum VTabDataFormats {
    CSV,
    AVRO,
    PARQUET,
    JSON,
    JSONL,
}

pub fn get_format(fmt: &str) -> Result<VTabDataFormats, Box<dyn Error>> {
    match fmt.to_uppercase().as_str() {
        "CSV" => Ok(VTabDataFormats::CSV),
        "AVRO" => Ok(VTabDataFormats::AVRO),
        "PARQUET" => Ok(VTabDataFormats::PARQUET),
        "JSON" => Ok(VTabDataFormats::JSON),
        "JSONL" => Ok(VTabDataFormats::JSONL),
        "NDJSON" => Ok(VTabDataFormats::JSONL),
        _ => Err(format!("Unknown data format: {}", fmt).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_format_valid() {
        assert_eq!(get_format("csv").unwrap(), VTabDataFormats::CSV);
        assert_eq!(get_format("AVRO").unwrap(), VTabDataFormats::AVRO);
        assert_eq!(get_format("parquet").unwrap(), VTabDataFormats::PARQUET);
        assert_eq!(get_format("JSON").unwrap(), VTabDataFormats::JSON);
        assert_eq!(get_format("jsonl").unwrap(), VTabDataFormats::JSONL);
        assert_eq!(get_format("NDJSON").unwrap(), VTabDataFormats::JSONL);
    }

    #[test]
    fn test_get_format_invalid() {
        let result = get_format("xml");
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.to_string(), "Unknown data format: xml");
        }
    }
}
